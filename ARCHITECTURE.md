# BoyoDB Architecture

This document describes the internal architecture of BoyoDB, a high-performance columnar database designed for analytics workloads, time-series data, and high-throughput OLAP applications.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client Layer                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  CLI Shell  │  Go Driver  │  Node.js  │  Python  │  Rust  │  C#  │  PHP    │
└──────┬──────┴──────┬──────┴─────┬─────┴────┬─────┴───┬────┴──┬───┴────┬────┘
       │             │            │          │         │       │        │
       └─────────────┴────────────┴──────────┴─────────┴───────┴────────┘
                                      │
                          TCP/TLS (JSON + Arrow IPC)
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            BoyoDB Server                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Request   │  │    Auth     │  │    Rate     │  │   Session   │        │
│  │   Router    │  │   Manager   │  │   Limiter   │  │   Manager   │        │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘        │
│         └────────────────┴────────────────┴────────────────┘                │
│                                   │                                          │
│                                   ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         Query Engine                                 │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐            │    │
│  │  │   SQL    │  │  Query   │  │  Query   │  │  Result  │            │    │
│  │  │  Parser  │  │ Planner  │  │ Executor │  │  Cache   │            │    │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘            │    │
│  └───────┴─────────────┴─────────────┴─────────────┴───────────────────┘    │
│                                   │                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                      Resource Governor                               │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐            │    │
│  │  │  Memory  │  │   I/O    │  │ Workload │  │  Query   │            │    │
│  │  │  Pools   │  │ Scheduler│  │  Groups  │  │ Throttle │            │    │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└───────────────────────────────────┼──────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            BoyoDB Core                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         Storage Engine                               │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐            │    │
│  │  │ Manifest │  │ Segment  │  │   WAL    │  │  Bloom   │            │    │
│  │  │  Index   │  │  Cache   │  │  Writer  │  │ Filters  │            │    │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘            │    │
│  └───────┴─────────────┴─────────────┴─────────────┴───────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         Cluster Module                               │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐            │    │
│  │  │  Gossip  │  │  Leader  │  │ Failover │  │  Write   │            │    │
│  │  │ Protocol │  │ Election │  │  Manager │  │  Quorum  │            │    │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                      Advanced Features                               │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐            │    │
│  │  │ Parquet  │  │   ORC    │  │  Delta   │  │ External │            │    │
│  │  │  Format  │  │  Format  │  │   Lake   │  │  Tables  │            │    │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Storage Tiers                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐                    │
│  │     Hot       │  │     Warm      │  │     Cold      │                    │
│  │   (NVMe/SSD)  │  │    (HDD)      │  │  (S3/Object)  │                    │
│  │               │  │               │  │               │                    │
│  │  Recent data  │  │  Older data   │  │  Archive      │                    │
│  │  Fast access  │  │  Medium I/O   │  │  Low cost     │                    │
│  └───────────────┘  └───────────────┘  └───────────────┘                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Module Overview

BoyoDB is organized into 17 feature phases, each adding enterprise capabilities:

| Phase | Module | Description |
|-------|--------|-------------|
| 1-4 | `engine.rs`, `sql.rs` | Core storage, SQL parser, query execution |
| 5 | `auth.rs` | Authentication, RBAC, password policies |
| 6 | `cluster/` | Gossip protocol, leader election, replication |
| 7 | `realtime.rs` | Streaming ingestion, CDC, materialized views |
| 8 | `operations.rs` | Backup/restore, online schema changes, TTL |
| 9 | `external.rs` | S3 integration, external tables, dictionaries |
| 10 | `security.rs` | Row-level security, column encryption, audit |
| 11 | `compression.rs` | Advanced codecs, dictionary/delta encoding |
| 12 | `mutations.rs` | Lightweight deletes, MergeTree engine |
| 13 | `observability.rs` | System tables, query profiling, Prometheus |
| 14 | `functions.rs` | Array, Map, JSON, Geo, IP, URL functions |
| 15 | `distributed.rs` | Distributed DDL, global indexes |
| 16 | `execution.rs` | Parallel execution, spill-to-disk, adaptive joins |
| 17 | `data_formats.rs` | Native Parquet/ORC, Delta Lake integration |
| 18 | `high_availability.rs` | Raft consensus, read replicas, failover |
| 19 | `resource_governance.rs` | Memory pools, I/O scheduling, workload groups |
| 20 | `tooling.rs` | Import/export, schema inference, CLI tools |

## Core Components

### Storage Engine

The storage engine uses a columnar format based on Apache Arrow IPC for efficient analytical queries.

#### Segments

Segments are the fundamental storage unit:
- Immutable once written
- Compressed with Zstd, LZ4, or Snappy
- CRC32 checksummed for integrity
- Stored as Arrow IPC stream format
- Organized into Hot/Warm/Cold tiers

```rust
struct ManifestEntry {
    segment_id: u64,
    database: String,
    table: String,
    shard_id: Option<u64>,
    row_count: u64,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
    watermark_micros: u64,
    min_event_time: Option<u64>,
    max_event_time: Option<u64>,
    bloom_filter: Option<Vec<u8>>,
    checksum: u32,
    tier: SegmentTier,  // Hot, Warm, Cold
    version_added: u64,
}
```

#### Manifest

The manifest is the metadata catalog tracking all segments:
- Persisted to `manifest.json`
- Indexed by (database, table) for O(1) lookup
- Versioned for incremental replication
- Backed up to WAL directory

#### Write-Ahead Log (WAL)

Ensures durability for ingest operations:
- Append-only log with buffered writes
- Automatic rotation when size exceeds threshold
- Replay on startup for crash recovery
- Configurable retention of rotated logs

### Query Engine

#### SQL Parser (`sql.rs`)

Uses `sqlparser-rs` with custom extensions:
- Standard SELECT/INSERT/UPDATE/DELETE
- DDL (CREATE/DROP/ALTER DATABASE/TABLE)
- Window functions (ROW_NUMBER, RANK, LAG, LEAD)
- CTEs (WITH clauses)
- Prepared statements
- UNION, INTERSECT, EXCEPT

#### Query Planner

Optimizes query execution:
- Segment pruning via bloom filters
- Predicate pushdown to storage
- Projection pruning
- Parallel scan planning
- Cost-based join ordering

#### Query Executor (`execution.rs`)

Executes queries using vectorized operations:
- Apache Arrow compute kernels
- Rayon-based parallelism
- Adaptive join selection (Hash, Merge, Broadcast)
- Spill-to-disk for large operations
- Pipeline-based execution model

```rust
pub struct QueryPipeline {
    stages: Vec<Box<dyn PipelineStage>>,
    config: ExecutionConfig,
    spill_manager: Option<Arc<SpillManager>>,
}

pub enum PipelineStage {
    Scan(ScanStage),
    Filter(FilterStage),
    Aggregate(AggregateStage),
    Sort(SortStage),
    Limit(LimitStage),
    Join(AdaptiveJoinExecutor),
}
```

### Authentication & Authorization (`auth.rs`)

#### User Management
- Argon2id password hashing
- Configurable password policy
- Account lockout protection
- Session-based authentication

#### Role-Based Access Control
- Built-in roles (admin, readonly, readwrite)
- Custom role creation
- Privilege inheritance
- Grant delegation

#### Privilege Model
```
Privilege Hierarchy:
├── SUPERUSER (all privileges)
├── ALL
│   ├── SELECT
│   ├── INSERT
│   ├── UPDATE
│   ├── DELETE
│   ├── CREATE
│   ├── DROP
│   ├── ALTER
│   ├── TRUNCATE
│   └── GRANT
└── Administrative
    ├── CREATEDB
    ├── CREATEUSER
    ├── CONNECT
    └── USAGE
```

### Cluster Module (`cluster/`)

#### Gossip Protocol (SWIM) - `gossip.rs`

Node discovery and failure detection:
- UDP-based protocol
- Periodic ping/ack exchanges
- Indirect probing for suspected nodes
- Phi Accrual failure detector

```rust
pub struct GossipProtocol {
    config: GossipConfig,
    membership: Membership,
    failure_detectors: HashMap<NodeId, PhiAccrualDetector>,
    pending_pings: HashMap<u64, Vec<PendingPing>>,
}
```

#### Leader Election (Raft-lite) - `election.rs`

Simplified Raft for leader election:
- Term-based voting
- Lease-based leadership
- Fencing tokens for consistency
- Automatic failover

```rust
pub struct ElectionState {
    term: u64,
    role: NodeRole,  // Leader, Follower, Candidate
    voted_for: Option<NodeId>,
    current_leader: Option<NodeId>,
    lease_expires: Option<Instant>,
    votes_received: HashSet<NodeId>,
}
```

#### Write Replication

Leader-to-follower replication:
- Synchronous quorum writes
- Asynchronous follower catch-up
- Bundle-based transfer format

### High Availability (`high_availability.rs`)

#### Read Replicas

```rust
pub struct ReplicaSelector {
    replicas: Vec<ReplicaInfo>,
    strategy: LoadBalanceStrategy,
}

pub enum LoadBalanceStrategy {
    RoundRobin,
    LeastConnections,
    LeastLatency,
    Random,
    Weighted,
}
```

#### Quorum Writes

```rust
pub struct QuorumWriter {
    consistency: WriteConsistency,
    timeout: Duration,
}

pub enum WriteConsistency {
    One,
    Quorum,
    All,
    LocalQuorum,
}
```

#### Health Monitoring

```rust
pub struct HealthMonitor {
    check_interval: Duration,
    failure_threshold: u32,
    recovery_threshold: u32,
}
```

### Resource Governance (`resource_governance.rs`)

#### Memory Pools

Hierarchical memory allocation with limits:
```rust
pub struct MemoryPool {
    name: String,
    max_bytes: usize,
    allocated: AtomicUsize,
    reserved: AtomicUsize,
    children: Vec<Arc<MemoryPool>>,
}
```

#### I/O Scheduling

Priority-based I/O with rate limiting:
```rust
pub struct IoScheduler {
    queues: HashMap<IoPriority, VecDeque<IoRequest>>,
    rate_limiter: RateLimiter,
}

pub enum IoPriority {
    Critical,
    High,
    Normal,
    Low,
    Background,
}
```

#### Workload Groups

Resource isolation for multi-tenant scenarios:
```rust
pub struct WorkloadGroup {
    name: String,
    max_memory_bytes: usize,
    max_cpu_percent: u32,
    max_concurrent_queries: usize,
    priority: WorkloadPriority,
}
```

### Data Formats (`data_formats.rs`)

#### Native Parquet Support

```rust
pub struct NativeParquetReader {
    schema: ParquetSchemaDef,
    predicate: Option<NativeParquetPredicate>,
    projection: Option<Vec<String>>,
}
```

#### Native ORC Support

```rust
pub struct NativeOrcReader {
    schema: OrcSchema,
    predicate: Option<OrcPredicate>,
    projection: Option<Vec<String>>,
}
```

#### Delta Lake Integration

```rust
pub struct DataLakeTable {
    location: String,
    format: TableFormat,  // Delta, Iceberg, Hudi
    snapshots: Vec<TableSnapshot>,
    transaction_log: Vec<TransactionLogEntry>,
}
```

### Advanced Functions (`functions.rs`)

| Category | Functions |
|----------|-----------|
| Array | `arrayJoin`, `arrayMap`, `arrayFilter`, `arrayReduce` |
| Map | `mapKeys`, `mapValues`, `mapContains` |
| JSON | `JSONExtract`, `JSONPath`, `JSONArrayLength` |
| Geo | `geoDistance`, `pointInPolygon`, `geohashEncode` |
| IP | `IPv4NumToString`, `IPv4StringToNum`, `IPv4CIDRToRange` |
| URL | `domain`, `path`, `extractURLParameter` |

### Security (`security.rs`)

#### Row-Level Security

```rust
pub struct RlsPolicy {
    name: String,
    table: String,
    command: RlsCommand,  // Select, Insert, Update, Delete, All
    expression: RlsExpression,
    policy_type: RlsPolicyType,  // Permissive, Restrictive
}
```

#### Column Encryption

```rust
pub struct ColumnEncryptionPolicy {
    column: String,
    algorithm: EncryptionAlgorithm,  // AES256GCM, ChaCha20Poly1305
    key_id: String,
}
```

#### Audit Logging

```rust
pub struct AuditEntry {
    timestamp: u64,
    event_type: AuditEventType,
    user: String,
    database: Option<String>,
    query: Option<String>,
    severity: AuditSeverity,
}
```

## Data Flow

### Ingest Path

```
Client Request
      │
      ▼
┌─────────────┐
│ Validation  │ ── Check auth, size limits, schema
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Resource   │ ── Memory pool allocation
│   Check     │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  WAL Write  │ ── Append to write-ahead log
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Segment   │ ── Compress and write Arrow IPC
│   Write     │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Manifest   │ ── Update metadata index
│   Update    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Replication │ ── Quorum write to followers
└─────────────┘
```

### Query Path

```
SQL Query
    │
    ▼
┌─────────────┐
│   Parse     │ ── SQL to AST
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Admission  │ ── Check workload group limits
│   Control   │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Plan      │ ── Check query/plan cache
│   Cache     │
└──────┬──────┘
       │ (miss)
       ▼
┌─────────────┐
│  Segment    │ ── Use manifest index + bloom filters
│  Selection  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Parallel   │ ── Rayon thread pool
│   Scan      │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Filter    │ ── Apply WHERE predicates (vectorized)
│   & Project │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Aggregate  │ ── GROUP BY, window functions
│   & Sort    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Result    │ ── Arrow IPC stream
│   Stream    │
└─────────────┘
```

## Performance Characteristics

### Time Complexity

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Point query | O(log n) | Bloom filter + index lookup |
| Range scan | O(k) | k = matching segments |
| Ingest | O(1) | Append-only |
| Manifest lookup | O(1) | Hash index |
| Aggregation | O(n) | Linear scan with SIMD |
| Join (Hash) | O(n + m) | Build + probe phases |
| Join (Sort-Merge) | O(n log n + m log m) | Sorted inputs |

### Space Complexity

| Component | Size | Notes |
|-----------|------|-------|
| Segment | Variable | Compressed with zstd (~3-10x ratio) |
| Manifest | O(segments) | ~200 bytes per segment |
| Bloom filter | 10KB/segment | ~1% false positive rate |
| Query cache | Configurable | Default 10K entries |
| Memory pool | Configurable | Default 4GB |

### Concurrency Model

- **Ingest**: Single writer with manifest lock (minimized critical section)
- **Query**: Multiple readers, lock-free segment access
- **Cluster**: Leader handles writes, any node handles reads
- **Resource**: Per-workload-group concurrency limits

## File Formats

### Manifest (manifest.json)

```json
{
  "format_version": 1,
  "version": 42,
  "databases": ["analytics", "production"],
  "tables": [
    {
      "database": "analytics",
      "table": "events",
      "schema_json": "{...}",
      "compression": "zstd"
    }
  ],
  "entries": [
    {
      "segment_id": 1,
      "database": "analytics",
      "table": "events",
      "row_count": 10000,
      "compressed_bytes": 102400,
      "watermark_micros": 1705312800000000,
      "tier": "Hot",
      "checksum": 3829472634
    }
  ]
}
```

### Segment (*.ipc)

Apache Arrow IPC stream format:
- Schema message (field names, types, nullability)
- Record batch messages (columnar data)
- Optional dictionary messages (for string columns)
- Compression wrapper (zstd/lz4/snappy)

### WAL Entry

```
┌────────────┬────────────┬────────────┬────────────────┐
│ Length (4) │ Type (1)   │ CRC32 (4)  │ Payload (var)  │
└────────────┴────────────┴────────────┴────────────────┘

Types:
  0x01 = Ingest
  0x02 = CreateDatabase
  0x03 = CreateTable
  0x04 = DropDatabase
  0x05 = DropTable
  0x06 = TruncateTable
  0x07 = AlterTable
```

## Network Protocol

### Frame Format

```
┌─────────────────┬───────────────────────┐
│  Length (4 BE)  │  JSON Payload (UTF-8) │
└─────────────────┴───────────────────────┘
```

### Request Types

| Operation | Description |
|-----------|-------------|
| `query` | Execute SELECT query |
| `ingest_ipc` | Ingest Arrow IPC data |
| `ingest_csv` | Ingest CSV data |
| `create_database` | Create database |
| `create_table` | Create table with schema |
| `explain` | Get query plan |
| `cluster_status` | Get cluster state |

### Response Format

```json
{
  "status": "ok",
  "ipc_base64": "...",
  "segments_scanned": 5,
  "execution_time_ms": 42
}
```

## Extension Points

### Custom Types

Implement `ArrowType` trait for new types:
```rust
pub trait ArrowType {
    fn arrow_data_type() -> DataType;
    fn to_arrow(&self) -> ArrayRef;
    fn from_arrow(array: &dyn Array, index: usize) -> Self;
}
```

### Custom Functions

Register scalar functions:
```rust
engine.register_function("my_func", |args| {
    // Implementation
});
```

### Storage Backends

The storage layer is abstracted:
- Local filesystem (default)
- S3-compatible object storage
- Azure Blob Storage
- Google Cloud Storage
- Custom backends via `ObjectStore` trait

## Security Model

### Network Security

- TLS 1.3 for encryption in transit
- mTLS for mutual authentication
- Token-based request authentication
- Rate limiting on all endpoints

### Data Security

- Argon2id password hashing
- Role-based access control
- Row-level security policies
- Column-level encryption
- Audit logging

### Operational Security

- Rate limiting on authentication
- Account lockout protection
- Session timeout
- Decompression bomb protection
- Query timeout enforcement

## Deployment Modes

### Standalone

Single-node deployment for development/testing:
```bash
boyodb-server /data 0.0.0.0:8765
```

### Cluster (3+ nodes)

Production HA deployment:
```bash
boyodb-server /data 0.0.0.0:8765 \
  --cluster \
  --cluster-id prod \
  --gossip-addr 0.0.0.0:8766 \
  --seed-nodes "node2:8766,node3:8766"
```

### 2-Node (Primary/Replica)

Simple HA with relaxed quorum:
```bash
boyodb-server /data 0.0.0.0:8765 \
  --cluster \
  --two-node-mode \
  --seed-nodes "replica:8766"
```

## Key Differentiators

| Aspect | BoyoDB |
|--------|--------|
| Language | Rust (memory-safe, zero-cost abstractions) |
| Storage Format | Apache Arrow IPC (industry standard) |
| Compression | Zstd, LZ4, Snappy with dictionary encoding |
| Clustering | Built-in Raft-lite (no external dependencies) |
| Query Language | PostgreSQL-compatible SQL |
| Data Lake | Native Delta Lake, Iceberg, Hudi support |
| Resource Management | Built-in memory pools, I/O scheduling, workload groups |
