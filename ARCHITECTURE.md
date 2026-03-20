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
| 21 | `transaction.rs`, `mvcc.rs` | ACID transactions, snapshot isolation |
| 22 | `optimizer_integration.rs` | Cost-based query optimizer |
| 23 | `cluster/distributed_recovery.rs` | Cross-node crash recovery |
| 24 | `pitr.rs`, `wal_archive.rs` | Point-in-time recovery |
| 25-28 | `range_types.rs`, `network_types.rs` | Advanced data types |
| 29-32 | `sync_replication.rs`, `connection_pooler.rs` | Replication, Operational Excellence |

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

## Performance Optimizations

BoyoDB includes several adaptive performance optimizations that automatically tune themselves based on workload characteristics.

### Adaptive Cache Sharding

The segment cache dynamically adjusts shard count based on CPU cores and cache size:

```rust
fn calculate_optimal_shards(num_cpus: usize, cache_size_bytes: u64) -> usize {
    let base = num_cpus * 3;
    let size_factor = if cache_size_bytes > 1_073_741_824 { 2 }      // >1GB
                      else if cache_size_bytes > 268_435_456 { 1 }  // >256MB
                      else { 0 };
    let shards = base + (base * size_factor) / 2;
    shards.clamp(16, 256)
}
```

**Benefits**:
- Reduces lock contention on multi-core systems
- Scales with available memory for larger deployments
- Automatically optimizes for read-heavy or write-heavy workloads

### Adaptive Bloom Filter FPP

Bloom filters use adaptive false positive probability based on data characteristics:

```rust
pub fn calculate_adaptive_fpp(
    row_count: usize,
    distinct_count: Option<u64>,
    segment_size_bytes: u64,
) -> f64 {
    let mut fpp = 0.01;  // 1% baseline

    // High cardinality (>80% distinct) -> tighter FPP for better pruning
    if let Some(distinct) = distinct_count {
        let ratio = distinct as f64 / row_count.max(1) as f64;
        if ratio > 0.8 { fpp = 0.005; }      // 0.5%
        else if ratio < 0.1 { fpp = 0.02; }  // 2%
    }

    // Large segments benefit more from tighter FPP
    if segment_size_bytes > 10_485_760 { fpp = fpp.min(0.005); }  // >10MB
    if segment_size_bytes < 102_400 { fpp = fpp.max(0.02); }      // <100KB

    fpp.clamp(0.001, 0.05)
}
```

**Benefits**:
- 2-5% better segment pruning for high-cardinality columns
- Reduced memory overhead for small segments
- Better I/O savings for large segments

### Parallel Merge Tree for Aggregations

Aggregation results from multiple segments use parallel tree reduction:

```
Sequential (8+ partials):     Parallel Tree Reduction:
    ┌─┬─┬─┬─┬─┬─┬─┬─┐              ┌─┬─┐ ┌─┬─┐ ┌─┬─┐ ┌─┬─┐
    │ │ │ │ │ │ │ │ │              └─┬─┘ └─┬─┘ └─┬─┘ └─┬─┘
    └─┴─┴─┴─┴─┴─┴─┴─┘   →           └──┬──┘     └──┬──┘
           ↓                            └────┬────┘
      [merge all]                           ↓
                                        [final]
```

```rust
pub fn merge_parallel(partials: Vec<Vec<u8>>, plan: AggPlan) -> Result<Vec<u8>, EngineError> {
    if partials.len() <= 8 {
        return merge_sequential(partials, plan);
    }
    // Parallel tree reduction using rayon
    current.par_chunks(2).map(|pair| merge(pair)).collect()
}
```

**Benefits**:
- 5-10% speedup for aggregations across many segments
- Logarithmic merge depth instead of linear
- Automatic fallback to sequential for small result sets

### Prefetch Integration with Cache

Proactive segment loading during sequential scans:

```
Query Scan:      ┌───┐ ┌───┐ ┌───┐ ┌───┐ ┌───┐ ┌───┐
                 │ 1 │ │ 2 │ │ 3 │ │ 4 │ │ 5 │ │ 6 │
                 └───┘ └───┘ └───┘ └───┘ └───┘ └───┘
                   ↑           ↑
                 scan        prefetch
                 here        these
```

```rust
fn prefetch_segments(&self, segment_ids: Vec<String>) {
    if !self.cfg.prefetch_enabled { return; }
    rayon::spawn(move || {
        for seg_id in segment_ids {
            if cache.get(&seg_id).is_some() { continue; }
            if let Ok(data) = storage.load_segment(&entry) {
                cache.insert(seg_id, Arc::new(data));
            }
        }
    });
}
```

**Benefits**:
- Improved cache hit rates for range queries
- Overlapped I/O with query processing
- Configurable prefetch depth via `prefetch_segments` config

### Parallel Compression Pipeline

Batch ingestion uses parallel compression across multiple batches:

```
Sequential:       Parallel Pipeline:
  ┌────────┐       ┌────────┐ ┌────────┐ ┌────────┐
  │ batch1 │       │ batch1 │ │ batch2 │ │ batch3 │
  │compress│       │compress│ │compress│ │compress│
  └────────┘       └────────┘ └────────┘ └────────┘
      ↓                 ↓          ↓          ↓
  ┌────────┐       ────────────────────────────────
  │ batch2 │                     parallel
  │compress│
  └────────┘
```

```rust
// Multi-batch ingestion compresses in parallel
let compressed: Vec<_> = batches
    .par_iter()
    .map(|batch| compress_payload(batch, compression))
    .collect();
```

**Benefits**:
- Reduced ingest latency for multi-batch operations
- Better CPU utilization during bulk loads
- Maintains sequential ordering for writes

### Zero-Copy Distributed Streaming

For massive inter-node queries (scatter-gather), the network protocol completely skips serialization overheads for large Arrow partitions:

```
[4-byte Length][JSON Response Metadata][Raw Binary Arrow IPC Payload]
```

**Benefits**:
- Bypasses Base64 allocation, eliminating massive string duplications for multi-hundred megabyte payloads
- Eradicates CPU decode/encode overhead loops entirely
- Trims network payload size strictly by ~33% exactly matching native raw sizes

### Configuration Options

```rust
pub struct EngineConfig {
    /// Adaptive shard count based on CPU/cache size (default: auto)
    pub segment_cache_shards: usize,

    /// Enable adaptive bloom filter FPP selection (default: true)
    pub adaptive_bloom_fpp: bool,

    /// Enable parallel compression for batch ingest (default: true)
    pub parallel_compression: bool,

    /// Number of segments to prefetch ahead (default: 4)
    pub prefetch_segments: usize,

    /// Enable prefetch during scans (default: true)
    pub prefetch_enabled: bool,
}
```

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

## Financial-Grade Features

BoyoDB includes enterprise features for financial and mission-critical workloads.

### ACID Transactions

```
┌─────────────────────────────────────────────────────────────┐
│                  Transaction Manager                         │
├─────────────────────────────────────────────────────────────┤
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌──────────┐ │
│  │  Active   │  │   Lock    │  │   Undo    │  │   MVCC   │ │
│  │   Txns    │  │  Manager  │  │    Log    │  │ Snapshots│ │
│  └───────────┘  └───────────┘  └───────────┘  └──────────┘ │
└─────────────────────────────────────────────────────────────┘
```

- **Transaction Manager**: Coordinates BEGIN, COMMIT, ROLLBACK
- **Lock Manager**: Hierarchical locking with deadlock detection
- **Undo Log**: Enables rollback and savepoints
- **MVCC**: Snapshot isolation with version visibility rules

### Cost-Based Query Optimizer

```
┌─────────────────────────────────────────────────────────────┐
│                  Query Optimizer                             │
├─────────────────────────────────────────────────────────────┤
│  ParsedQuery → LogicalPlan → PhysicalPlan → Execution       │
│                                                              │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐               │
│  │Statistics │  │   Cost    │  │  Index    │               │
│  │ Collector │  │ Estimator │  │ Selection │               │
│  └───────────┘  └───────────┘  └───────────┘               │
└─────────────────────────────────────────────────────────────┘
```

- Collects table and column statistics
- Estimates query costs based on row counts and selectivity
- Automatic index selection for range queries
- Predicate pushdown optimization

### Distributed Crash Recovery

```
┌─────────────────────────────────────────────────────────────┐
│              Distributed Recovery Manager                    │
├─────────────────────────────────────────────────────────────┤
│  Node States: Healthy → Suspected → Crashed → Recovering    │
│                                                              │
│  Recovery Actions:                                           │
│  - RecoverFromLeader (small lag)                            │
│  - RestoreFromBackup (large lag)                            │
│  - FullRebuild (last resort)                                │
└─────────────────────────────────────────────────────────────┘
```

- Automatic crash detection via heartbeat monitoring
- Cross-node recovery coordination
- Multiple recovery strategies based on WAL lag
- Local crash recovery with persistent metadata

### Point-in-Time Recovery (PITR)

```
┌─────────────────────────────────────────────────────────────┐
│                    PITR System                               │
├─────────────────────────────────────────────────────────────┤
│  Base Backup ─────► WAL Archive ─────► Target Time          │
│       │                   │                   │              │
│  CREATE BACKUP      LSN Tracking      RECOVER TO            │
└─────────────────────────────────────────────────────────────┘
```

- Create base backups with `CREATE BACKUP`
- WAL archiving with Log Sequence Numbers (LSN)
- Recover to any timestamp or specific LSN
- Supports local and S3 archive storage

## Fault Tolerance

BoyoDB includes multiple layers of fault tolerance to protect against data corruption and ensure reliability.

### Checksum Validation

```
┌─────────────────────────────────────────────────────────────┐
│                  Data Integrity Layers                       │
├─────────────────────────────────────────────────────────────┤
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌──────────┐ │
│  │  Segment  │  │    WAL    │  │  Manifest │  │ Checksum │ │
│  │ Checksums │  │ Checksums │  │ Checksums │  │  Journal │ │
│  └───────────┘  └───────────┘  └───────────┘  └──────────┘ │
└─────────────────────────────────────────────────────────────┘
```

- **Segment Checksums**: xxHash64 checksums on all data segments
- **WAL Checksums**: CRC32 on every WAL record
- **Manifest Checksums**: Integrity verification of metadata
- **Checksum Journal**: Redundant independent checksum tracking

### IPC Format Validation

Deep validation of Arrow IPC format during loads:

```rust
pub fn validate_ipc_format(ipc: &[u8]) -> Result<bool, EngineError> {
    // Validates:
    // - IPC header and footer structure
    // - Record batch integrity
    // - Schema consistency
}
```

### Auto-Repair

Automatic recovery from detected corruption:

```rust
pub struct EngineConfig {
    pub auto_repair_on_corruption: bool,
    pub verify_s3_uploads: bool,
    pub segment_operation_max_retries: usize,
    pub segment_operation_retry_delay_ms: u64,
}
```

### S3 Upload Verification

Read-back verification for cold storage:

```rust
pub fn persist_segment_cold_with_verify(
    &self,
    segment_id: &str,
    data: &[u8],
    verify: bool,
) -> Result<(), EngineError> {
    // 1. Upload to S3
    // 2. Read back uploaded data
    // 3. Compare checksums
}
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
| Transactions | Full ACID with MVCC and snapshot isolation |
| Recovery | Point-in-time recovery with WAL archiving |
| Fault Tolerance | Multi-layer checksums, auto-repair, S3 verification |

## Connection Pooler (`connection_pooler.rs`)

BoyoDB includes a built-in connection pooler similar to PgBouncer for efficient connection management.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Connection Pooler                           │
├─────────────────────────────────────────────────────────────┤
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌──────────┐ │
│  │  Client   │  │   Pool    │  │  Server   │  │  Admin   │ │
│  │ Tracking  │  │  Manager  │  │ Connections│  │ Console  │ │
│  └───────────┘  └───────────┘  └───────────┘  └──────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Pool Modes

```rust
pub enum PoolMode {
    Transaction,  // Return to pool after each transaction (default)
    Session,      // Hold connection for entire client session
    Statement,    // Return to pool after each statement (most aggressive)
}
```

### Authentication

- **MD5 Password**: PostgreSQL-compatible `md5(md5(password + user) + salt)` hash
- **Plain Text**: Simple password verification (not recommended)
- **Trust**: No authentication required

```rust
pub fn compute_md5_password(password: &str, user: &str, salt: &[u8; 4]) -> String {
    // PostgreSQL MD5 authentication
}
```

### Admin Commands

| Command | Description |
|---------|-------------|
| `SHOW STATS` | Global pooler statistics |
| `SHOW POOLS` | Per-database/user pool info |
| `SHOW CLIENTS` | Connected client list |
| `SHOW SERVERS` | Backend connection list |
| `SHOW CONFIG` | Current configuration |
| `SHOW PAUSED` | Paused databases |
| `PAUSE <db>` | Stop new connections to database |
| `RESUME <db>` | Resume connections to database |
| `WAIT <db>` | Wait for active queries to finish |
| `KILL <db>` | Close all connections to database |
| `RELOAD` | Reload configuration from file |
| `SET param = value` | Change runtime parameter |

### Configuration

PgBouncer-compatible INI format:

```ini
[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432
max_client_conn = 1000
default_pool_size = 20
pool_mode = transaction

[databases]
mydb = host=localhost port=5432 dbname=mydb
```

## Memory Context (`memory_context.rs`)

PostgreSQL-style hierarchical memory allocation tracking for efficient resource management.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                Memory Context Hierarchy                      │
├─────────────────────────────────────────────────────────────┤
│  TopLevel                                                    │
│  ├── Query (per query)                                       │
│  │   ├── Sort (work_mem limited)                            │
│  │   ├── Hash (work_mem limited)                            │
│  │   └── Aggregate                                           │
│  ├── Transaction (per transaction)                          │
│  └── Cache (long-lived)                                      │
└─────────────────────────────────────────────────────────────┘
```

### Context Types

```rust
pub enum MemoryContextType {
    TopLevel,     // Global context
    Query,        // Per-query allocations
    Transaction,  // Transaction-scoped memory
    Expression,   // Expression evaluation
    Operation,    // Sort/hash operations (work_mem limited)
    Cache,        // Long-lived cached data
    Temporary,    // Short-lived allocations
}
```

### Memory Tracking

- **Atomic allocation**: Thread-safe memory tracking with compare-and-swap
- **Limit enforcement**: Per-context memory limits with automatic rejection
- **Slot reuse**: Efficient slab-style allocator that reuses freed slots
- **Statistics**: Total allocated, current used, peak usage, allocation counts

```rust
pub struct MemoryContext {
    name: String,
    context_type: MemoryContextType,
    limit: AtomicUsize,
    stats: MemoryContextStats,
    blocks: RwLock<Vec<Option<MemoryBlock>>>,
    free_slots: RwLock<Vec<usize>>,
}
```

### Usage Pattern

```rust
// Create query context with work_mem limit
let query_ctx = manager.create_query_context();
query_ctx.context().set_limit(64 * 1024 * 1024);

// Allocate in sort sub-context
let sort_ctx = query_ctx.sort_context();
let ptr = sort_ctx.alloc(1024)?;

// Free individual allocations
sort_ctx.dealloc(ptr);

// Reset entire context (frees all allocations)
query_ctx.reset();
```
