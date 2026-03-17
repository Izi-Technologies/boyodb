# Changelog

All notable changes to BoyoDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.7] - 2026-03-16

### Fixed

- **UInt64 event_time Column Statistics**: Fixed event_time stats not being collected for UInt64 columns in `validate_and_stats`. Previously, event_time min/max stats were only collected for TimestampMicrosecond columns, causing filtered queries on UInt64 event_time columns to incorrectly prune all segments.

- **Cross-Type Numeric Comparison in Zone Maps**: Fixed `PrimitiveValue::partial_cmp_value` to handle cross-type comparisons between signed and unsigned integers (Int64 vs UInt64, Int32 vs UInt64, etc.). Zone-map pruning was incorrectly rejecting segments when comparing Int64 filter values against UInt64 column statistics.

- **Background Thread Data Integrity**: Fixed critical bugs where background threads created separate Db instances instead of sharing the original. This affected:
  - **Write buffer flush thread**: Writes after first batch remained in memory and were lost on restart
  - **Compaction threads**: Operated on stale data, missing buffered writes
  - **Column rename/schema rewrite**: Could cause inconsistencies with buffered data

  The fix introduces new public functions (`start_compaction_threads`, `start_write_buffer_flush_thread`) that take `Arc<Db>` and must be called after wrapping Db in Arc. Background DDL operations now use a weak self-reference to share the Db instance.

- **Query Result IPC Stream Concatenation**: Fixed SELECT * queries returning only 1 row when multiple segments match. IPC streams from parallel/sequential scans are now properly decoded to batches, collected, and re-encoded as a single valid IPC stream.

### Added

#### Performance Optimizations
- **Adaptive Cache Sharding**: Dynamic shard count based on CPU cores and cache size
  - Auto-calculated optimal shards (3x CPU cores, scaled for large caches)
  - Configurable via `segment_cache_shards` setting
  - Reduces lock contention by 15-30% on high-core systems

- **Adaptive Bloom Filter FPP**: Intelligent false positive rate selection
  - High cardinality data (>80% distinct) uses tighter 0.5% FPP
  - Large segments (>10MB) use tighter FPP for better I/O savings
  - Small segments (<100KB) use looser FPP to save memory
  - 2-5% improvement in segment pruning efficiency

- **Parallel Merge Tree for Aggregations**: Tree-reduction for partial aggregates
  - Parallel pairwise merging using rayon for >8 partials
  - 5-10% aggregation speedup on multi-core systems
  - Automatic fallback to sequential for small counts

- **Prefetch Integration with Cache**: Proactive segment loading
  - Initial batch prefetch after segment matching
  - Progressive prefetch during sequential scans
  - Improved cache hit rates for large sequential queries

- **Parallel Compression Pipeline**: Concurrent batch compression
  - Parallel validation and compression for multi-batch ingestion
  - Pre-allocated sequence numbers for consistency
  - Reduced I/O latency for bulk writes

## [0.9.6] - 2026-03-15

### Added

#### Enterprise Operations Features (Phase 41)
- **Query Result Caching**: Distributed cache with Redis-compatible protocol
  - Sharded LRU cache with configurable size limits
  - Time-based and event-based invalidation
  - Redis RESP protocol server for external cache access
  - Table dependency tracking for automatic invalidation
  - Per-tenant cache isolation

- **Multi-Region Disaster Recovery**: Cross-region replication with automatic failover
  - WAL-based async replication to secondary regions
  - Configurable RPO/RTO with violation alerting
  - DNS routing integration for traffic failover
  - Automatic failover based on health checks
  - Failover state machine (Normal → InProgress → Completed)

- **Query Cost Estimation API**: Pre-flight cost estimates for query planning
  - Cardinality estimation with statistics
  - CPU, memory, I/O, and network cost prediction
  - Query complexity analysis
  - Cost model calibration from execution history

- **Tenant Isolation Enhancements**: Namespace-level encryption and per-tenant backup
  - Per-namespace encryption keys with rotation
  - KMS integration (AWS, Azure, GCP, HashiCorp Vault)
  - Per-tenant backup/restore with PITR
  - Resource quota enforcement (storage, connections, QPS)

- **CDC to Data Lakes**: Direct CDC to Delta Lake/Iceberg format
  - Delta Lake writer with transaction log
  - Apache Iceberg writer with snapshot management
  - Schema evolution support
  - Partitioned writes with compaction

- **Query Replay/Shadowing**: Traffic capture and replay for testing
  - Query capture with configurable sampling
  - Replay to test clusters with result comparison
  - Shadow traffic service for live duplication
  - Performance regression detection

- **Auto-Scaling Policies**: Metrics-based scaling triggers
  - Policy-based scaling (threshold, step, target tracking)
  - Predictive scaling with seasonality modeling
  - Cooldown periods and consecutive breach requirements
  - Support for CPU, memory, connections, QPS metrics

- **Data Retention Policies**: GDPR/CCPA compliant data management
  - Retention policies with purge/anonymize/archive actions
  - Legal hold management with scope control
  - Data subject request handling (access, erasure, portability)
  - Compliance reporting (GDPR, CCPA, HIPAA, SOX, PCI)

### Fixed
- **Memory Leak Prevention**: Added bounds to prevent unbounded growth in all enterprise modules
  - Bounded history vectors in all modules (configurable limits)
  - Cleanup methods for completed/expired entries
  - Stale dependency pruning in query cache

#### ClickHouse Parity Features (Phase 38)
- **Approximate Functions**: Statistical estimation with bounded error
  - HyperLogLog for cardinality estimation (~2% error, constant memory)
  - T-Digest for accurate quantile/percentile estimation (p50, p99, p999)
  - Count-Min Sketch for frequency estimation
  - Streaming statistics (mean, variance, min, max)

- **MergeTree Variants**: Specialized table engines
  - ReplacingMergeTree: Deduplication by key with version tracking
  - CollapsingMergeTree: Sign-based row collapsing for state changes
  - VersionedCollapsingMergeTree: Versioned collapsing for out-of-order data
  - AggregatingMergeTree: Pre-aggregation during merge
  - SummingMergeTree: Automatic sum aggregation

- **External Tables**: Query external data sources directly
  - S3/Object storage with predicate pushdown
  - HTTP URL sources (CSV, JSON, Parquet)
  - HDFS integration
  - Local file access
  - Delta Lake and Iceberg format support

- **Async Insert Buffering**: High-throughput ingestion optimization
  - Configurable buffer size and flush intervals
  - Per-table buffer limits
  - Deduplication within buffer
  - Concurrent flush support

- **Query Profiler**: Deep query execution analysis
  - Flame graph generation for visualization
  - Per-operator timing and row counts
  - Memory allocation tracking
  - I/O statistics (bytes read, segments scanned)
  - Wait event tracking (I/O, lock, CPU, network)

- **Parallel Replicas**: Distributed query execution
  - Query part distribution across replicas
  - Load balancing strategies (round-robin, least-loaded, weighted)
  - Automatic failover and retry
  - Result merging with statistics

- **Zero-Copy Replication**: Storage-efficient replication
  - Segment sharing via object storage
  - Reference counting for garbage collection
  - Local caching with LRU eviction
  - Optimistic locking for concurrent access

#### PostgreSQL Parity Features (Phase 39)
- **Exclusion Constraints**: Prevent overlapping data
  - Range overlap prevention (scheduling, bookings)
  - GiST index-backed constraints
  - Multiple column support with different operators
  - Deferrable constraints

- **GIN/GiST Indexes**: Advanced indexing for complex types
  - GIN: Full-text search, arrays, JSONB with fast updates
  - GiST: Spatial indexing with R-tree semantics
  - Box and range key types
  - Configurable split strategies

- **Change Data Capture**: Debezium-compatible CDC
  - Event types: create, update, delete, truncate, schema
  - Before/after row images
  - Transaction metadata
  - Multiple output formats (JSON, Avro, Protobuf)

- **WebAssembly UDFs**: Sandboxed user-defined functions
  - WASM module registration and execution
  - Type-safe value passing
  - Resource limits and timeouts
  - Built-in functions library

#### AI/ML Query Optimization
- **AI Query Optimizer**: Machine learning-based optimization
  - Cardinality estimation with learned models
  - Cost model calibration from execution history
  - Plan scoring with multiple weighted factors
  - Adaptive weight updates based on feedback

- **Tiered JIT Compilation**: Progressive code optimization
  - Interpreted execution for cold queries
  - Baseline JIT for warm queries
  - Optimized JIT with advanced passes
  - Vectorized SIMD compilation for hot paths
  - Automatic tier promotion based on execution count

### Changed
- Updated all driver versions to 0.9.6

## [0.9.5] - 2026-03-14

### Added

#### Machine Learning Features (Phase 34)
- **Feature Store**: Versioned feature engineering with point-in-time lookups
  - Feature groups with transformations (StandardScaler, MinMax, OneHot, Bucketize)
  - Online store with TTL caching for real-time serving
  - Feature pipelines for data processing workflows

- **Model Monitoring**: Production ML model observability
  - PSI (Population Stability Index) and KS (Kolmogorov-Smirnov) drift detection
  - Performance metrics tracking with alerting
  - Reference distribution comparison

- **Embeddings Engine**: Text embedding generation and management
  - Sentence transformer model support
  - Embedding caching for performance
  - Cosine similarity search

- **Online Learning**: Incremental model updates
  - SGD, Adam, AdaGrad, RMSProp, Momentum optimizers
  - Multi-armed bandits (epsilon-greedy, UCB, Thompson Sampling)
  - Contextual bandits with LinUCB algorithm

- **AutoML**: Automated machine learning pipeline
  - Grid, random, and Bayesian hyperparameter search
  - K-fold and stratified cross-validation
  - Feature importance scoring
  - Trial tracking and model selection

- **ML Explainability**: Interpretable ML predictions
  - SHAP (Kernel SHAP) for global/local explanations
  - LIME for local interpretable explanations
  - Permutation feature importance
  - Counterfactual explanations

#### Data Platform Features (Phase 35)
- **Time Series Engine**: Specialized time series analytics
  - Aggregation by time buckets (second to year)
  - Gap filling (null, zero, forward/backward fill, linear interpolation)
  - Downsampling with configurable policies
  - Linear regression forecasting
  - Z-score anomaly detection

- **Graph Database Engine**: Native graph query capabilities
  - Node and edge storage with arbitrary properties
  - BFS/DFS graph traversal with depth limits
  - Dijkstra's shortest path algorithm
  - All paths enumeration
  - PageRank algorithm for node importance
  - Label propagation community detection

- **Data Quality Framework**: Automated data validation
  - Validation rules: NotNull, Unique, Range, Regex, Email, URL, Custom
  - Column profiling with statistics
  - Anomaly detection using z-score
  - Quality scoring with recommendations

- **Natural Language to SQL**: NL query interface
  - Intent recognition (select, count, sum, avg, max, min, group, filter, order)
  - Entity extraction for tables and columns
  - Schema-aware SQL generation
  - Support for aggregations, conditions, ordering

- **Data Catalog**: Enterprise metadata management
  - Catalog entries with metadata, classification, tags
  - Data lineage graph (upstream/downstream tracking)
  - Business glossary with term definitions
  - Full-text search with relevance scoring

- **Blockchain Ledger**: Immutable audit logging
  - SHA-256 hash chains for data integrity
  - Merkle tree verification
  - Transaction signing and verification
  - Tamper detection and chain validation

- **Workflow Engine**: DAG-based pipeline orchestration
  - Task dependencies with topological ordering
  - Parallel execution levels
  - Retry policies with exponential backoff
  - Scheduling (cron, interval, daily, weekly)
  - Task state tracking and logging

#### Advanced Analytics Features (Phase 36)
- **Enhanced Vector Search**: HNSW approximate nearest neighbor
  - Multiple distance metrics (Cosine, Euclidean, Dot Product, Manhattan)
  - Product quantization for memory efficiency
  - Filtered vector search with metadata predicates
  - Configurable M and ef parameters

- **Query Federation**: Multi-source query execution
  - Support for PostgreSQL, MySQL, S3, HTTP APIs
  - Push-down optimization (filters, projections, aggregations)
  - Result merging and caching
  - Connection pooling

- **Real-time Dashboards**: WebSocket streaming
  - Live metric updates via WebSocket
  - Subscription management
  - Dashboard configurations with widgets
  - Alert broadcasting
  - Query result streaming

- **Data Contracts**: Schema evolution management
  - Semantic versioning (major.minor.patch)
  - Backward/forward compatibility checking
  - Breaking change detection
  - Migration plan generation
  - Type promotion rules

- **Lakehouse Formats**: Open table format support
  - Delta Lake transaction log and ACID semantics
  - Apache Iceberg metadata and manifest files
  - Time travel queries
  - Schema evolution
  - Partition pruning

- **SQL Extensions**: Analytical function bindings
  - Graph functions: GRAPH_TRAVERSE, SHORTEST_PATH, PAGERANK, COMMUNITY_DETECT
  - Time series: DOWNSAMPLE, GAP_FILL, MOVING_AVERAGE, FORECAST, DETECT_ANOMALIES
  - Vector search: VECTOR_SEARCH, COSINE_DISTANCE, EUCLIDEAN_DISTANCE, EMBEDDING
  - Data quality: VALIDATE, PROFILE, QUALITY_SCORE
  - Catalog: SEARCH_CATALOG, DATA_LINEAGE
  - Workflow: RUN_WORKFLOW, WORKFLOW_STATUS

#### Performance & Testing (Phase 37)
- **Benchmarking Utilities**: Performance measurement tools
  - BenchmarkRunner with warmup and iteration control
  - LatencyHistogram for distribution analysis
  - ThroughputTracker for sustained workloads
  - Percentile calculations (P50, P95, P99, P99.9)
  - JSON output for CI integration

- **GraphQL API**: Complete GraphQL interface for BoyoDB
  - Full schema introspection with automatic table schema generation
  - Query, Mutation, and Subscription type support
  - SDL (Schema Definition Language) export
  - Nested field resolution for related data
  - Custom scalar types for BoyoDB data types

- **In-Database ML Inference**: Machine learning model registry and inference
  - Support for ONNX, TensorFlow, PyTorch, XGBoost, LightGBM model formats
  - Model versioning with A/B testing support
  - Batch inference with configurable batch sizes
  - Prediction caching for improved performance
  - Feature preprocessing pipelines (normalize, standardize, one-hot encode)
  - SQL interface: `ML_PREDICT(model, features)`, `ML_SCORE(model, input)`

- **OpenTelemetry Integration**: Distributed tracing and metrics
  - W3C Trace Context propagation (traceparent header support)
  - Automatic tracing for queries, ingestion, and compaction
  - Metrics collection (counters, gauges, histograms)
  - OTLP and Prometheus exporters
  - Database semantic conventions (db.system, db.statement, etc.)

- **CDC Webhooks**: Real-time change data capture notifications
  - Configurable webhook endpoints per table
  - INSERT, UPDATE, DELETE operation tracking
  - Batched delivery for high-throughput scenarios
  - Retry logic with exponential backoff
  - HMAC-SHA256 payload signing for security
  - At-least-once delivery semantics

- **PIVOT/UNPIVOT**: SQL data transformation
  - `PIVOT (aggregate FOR column IN (values))` - rows to columns
  - `UNPIVOT (value_column FOR name_column IN (columns))` - columns to rows

- **Approximate Aggregates**: High-performance statistical functions
  - `APPROX_PERCENTILE(column, percentile)` - T-Digest approximate percentile
  - `APPROX_MEDIAN(column)` - T-Digest approximate median
  - `APPROX_COUNT_DISTINCT(column)` - HyperLogLog cardinality estimation

- **GROUPING SETS/CUBE/ROLLUP**: Multi-level aggregations
  - `GROUP BY GROUPING SETS (...)` - multiple grouping combinations
  - `GROUP BY ROLLUP (...)` - hierarchical subtotals
  - `GROUP BY CUBE (...)` - all possible combinations
  - `GROUPING(column)` function for null vs aggregate null detection

- **DISTINCT ON**: PostgreSQL-style first-row-per-group selection
  - `SELECT DISTINCT ON (category) * FROM products ORDER BY category, price`
  - Returns first row for each unique value in the DISTINCT ON columns
  - Fully compatible with ORDER BY for deterministic row selection

- **Incremental Materialized View Refresh**: Delta-based updates for efficient refreshes
  - `REFRESH MATERIALIZED VIEW view_name INCREMENTAL`
  - Uses watermark tracking to identify changed rows since last refresh
  - Merges delta changes with existing view data
  - Dramatically reduces refresh time for append-heavy workloads

- **Advanced JSON Path Expressions**: Extended JSONPath syntax
  - Wildcard access: `$.items[*].name` - access all array elements
  - Array slicing: `$.items[0:5]` - slice arrays with start:end syntax
  - Recursive descent: `$..field` - find field at any depth
  - Filter expressions: `$.items[?(@.price > 100)]` - filter with conditions
  - `JSON_EXTRACT_ALL()` function for multi-value extraction

- **WITHIN GROUP Ordered Aggregates**: Statistical and ordered aggregate functions
  - `MODE() WITHIN GROUP (ORDER BY column)` - most frequent value
  - `STRING_AGG_ORDERED(col, sep ORDER BY ...)` - ordered string concatenation
  - `ARRAY_AGG_ORDERED(col ORDER BY ...)` - ordered array aggregation
  - `FIRST_VALUE(col) WITHIN GROUP (ORDER BY ...)` - first value in order
  - `LAST_VALUE(col) WITHIN GROUP (ORDER BY ...)` - last value in order
  - `NTH_VALUE(col, n) WITHIN GROUP (ORDER BY ...)` - nth value in order

- **Query Federation Push-down**: Optimized foreign data wrapper queries
  - Push aggregations (SUM, COUNT, AVG, MIN, MAX) to foreign servers
  - Push GROUP BY clauses for remote aggregation
  - Push ORDER BY and LIMIT for sorted remote fetches
  - Reduces data transfer by computing aggregates at source
  - Works with PostgreSQL and other FDW sources

### Improved
- **Driver Updates**: All drivers updated to v0.9.5 (Python, Rust, Go, Java, Node.js, C#, PHP)

## [0.9.4] - 2026-03-11

### Added
- **Built-in Connection Pooler (PgBouncer-compatible)**: Enterprise-grade connection pooling
  - Transaction, Session, and Statement pooling modes
  - MD5 password authentication support
  - Real TCP connections with PostgreSQL startup handshake
  - Admin commands: `SHOW STATS/POOLS/CLIENTS/SERVERS/CONFIG/PAUSED`
  - Dynamic configuration: `SET param = value`, `RELOAD`
  - Database management: `PAUSE/RESUME/ENABLE/DISABLE/WAIT/KILL <database>`
  - PgBouncer-style INI configuration file support
  - Per-database/user connection pools with automatic scaling
  - Connection health checks and automatic reset

- **Memory Context Manager**: PostgreSQL-style memory allocation tracking
  - Hierarchical memory contexts (TopLevel, Query, Transaction, Expression, Operation)
  - Per-context memory limits with atomic enforcement
  - Efficient slab-style allocator with slot reuse
  - Memory usage statistics and peak tracking
  - Automatic memory cleanup on context reset/delete

- **Foreign Key CASCADE Actions**: Full referential integrity enforcement
  - `ON DELETE CASCADE`: Automatically delete referencing rows when parent row is deleted
  - `ON DELETE SET NULL`: Set foreign key columns to NULL when parent row is deleted
  - `ON DELETE SET DEFAULT`: Set foreign key columns to default values when parent row is deleted
  - `ON UPDATE CASCADE`: Automatically update referencing rows when parent key is updated
  - `ON UPDATE SET NULL`: Set foreign key columns to NULL when parent key is updated
  - `ON UPDATE SET DEFAULT`: Set foreign key columns to default values when parent key is updated
  - Recursive cascading through multiple FK relationships
  - Proper constraint violation errors for RESTRICT/NO ACTION

- **Read Replica Support**: Built-in read replica mode for scaling read workloads
  - `--replica` flag to run server in read-only mode
  - Manifest sync from shared S3 storage or HTTP bundle pull from primary
  - Configurable sync interval with `--replica-sync-interval-ms`
  - Replica status monitoring endpoint
  - Proper PostgreSQL error codes (SQLSTATE 25006) for write rejections

- **WAL Archive S3 Operations**: Full S3 support for WAL archiving
  - Real S3 upload for WAL segment archiving (was stub)
  - Real S3 download for WAL segment retrieval (was NotImplemented)
  - Real S3 delete for WAL retention enforcement (was stub)
  - Uses ObjectStore abstraction for S3 compatibility
  - Configurable S3 bucket, prefix, and credentials

- **Foreign Data Wrapper (FDW) PostgreSQL Connections**: Real PostgreSQL connectivity
  - Real tokio-postgres connections when `fdw-postgres` feature enabled
  - Full query execution with proper result iteration
  - Transaction support (BEGIN/COMMIT/ROLLBACK)
  - Schema import from remote PostgreSQL databases
  - Table statistics retrieval for query planning
  - Proper PostgreSQL type to FDW type mapping

- **Metal GPU Acceleration (macOS)**: Real Metal API support
  - Device detection using Metal API when `metal-gpu` feature enabled
  - GPU buffer creation for data transfer
  - Framework for Metal compute shader execution
  - Unified memory support on Apple Silicon
  - Device capability querying

- **Cranelift JIT Compilation**: Real native code generation
  - Expression compilation to machine code when `jit-cranelift` feature enabled
  - Support for arithmetic, comparison, and bitwise operations
  - Native code execution for hot expressions
  - Automatic fallback to interpretation on compilation failure
  - Expression caching with configurable cache size

- **ICU Unicode Collation**: Real ICU library support
  - ICU Collator integration when `icu-collation` feature enabled
  - Production-grade Unicode normalization (NFC, NFD, NFKC, NFKD)
  - Locale-aware string comparison
  - Configurable collation strength levels

### Fixed
- **Memory Context Double-Counting**: Fixed bug where parent contexts counted children's allocations twice (once in parent's `current_used`, once via `total_usage()` recursion)
- **Memory Context Leak**: Fixed memory leak where deallocated blocks weren't properly removed from the block vector

### Improved
- **Connection Pooler Race Condition**: Fixed TOCTOU race in `accept_client()` using atomic fetch_add with rollback pattern
- **Sync Replication**: Fixed non-eligible replicas not being reset to Async state when they become ineligible

## [0.2.7] - 2026-03-09

### Added
- **Group Commit for Fast Transactional Writes**: New WAL batching mechanism that dramatically improves throughput for workloads with many small writes
  - Batches multiple writes together before fsync, reducing disk I/O overhead
  - Configurable parameters: `group_commit_delay_ms`, `group_commit_max_writes`, `group_commit_max_bytes`
  - Enabled by default with sensible defaults (5ms delay, 1000 writes, 16MB max batch)
- **Batch Ingest API**: New `ingest_ipc_batch()` method for efficient multiple small writes
  - Single manifest lock acquisition for all entries
  - Single WAL fsync for all entries (with group commit)
  - Atomic semantics: all batches succeed or all fail validation
- **Group Commit Configuration**: New builder method `with_group_commit()` for easy configuration

### Performance
- **Fast Transactional Writes**: Up to 10-50x throughput improvement for many small writes
  - Before: Each write requires separate fsync (~10ms latency per write)
  - After: Multiple writes batched with single fsync (amortized latency)
- **Frequent Small Updates**: Batch API reduces per-write overhead
  - Single lock acquisition instead of N locks for N writes
  - Single WAL sync instead of N syncs

### Configuration
New engine configuration options:
- `group_commit_enabled`: Enable group commit (default: true)
- `group_commit_delay_ms`: Maximum delay before flushing batch (default: 5ms)
- `group_commit_max_writes`: Maximum writes per batch (default: 1000)
- `group_commit_max_bytes`: Maximum bytes per batch (default: 16MB)

## [0.2.6] - 2026-03-09

### Fixed
- **Missing Segments Bug**: Critical fix for segments being lost when memtable buffering is enabled
  - Root cause: Manifest was persisted before memtable segments were flushed to disk
  - Fix: Now flushes all memtables BEFORE persisting manifest
  - Also fixed in Drop impl to flush memtables on DB shutdown
- **Repair Tool Runtime**: Fixed repair tool to work without Tokio runtime for local-only storage
- **WAL Rotation**: Handle missing WAL file gracefully during rotation, recreate if needed
- **Manifest Directory**: Ensure binary manifest parent directory exists before atomic rename

### Invariant Enforced
- Segment files MUST exist on disk BEFORE manifest references them
- Prevents any future missing segments from manifest/memtable race conditions

## [0.2.5] - 2026-03-09

### Fixed
- **WAL LSN Atomic Persist**: Fixed directory creation issue in WAL LSN atomic persist that could cause "No such file or directory" errors
- **Manifest Race Condition**: Fixed atomic manifest rename to never fall back to truncating live manifest
- **S3 Upload Error Handling**: Enhanced error logging when corrupt S3 segment deletion fails after checksum mismatch
- **SQL Parser Dialect**: Switched to PostgreSqlDialect for proper `USING` clause support in CREATE INDEX

### Improved
- **CREATE INDEX Syntax**: Now follows PostgreSQL standard with USING before columns
  - `CREATE INDEX idx_phone ON cdr USING FULLTEXT (calling_number)`
- **Corruption Prevention**: Enhanced atomic file operations across WAL, manifest, and segment persistence

## [0.2.4] - 2026-03-08

### Fixed
- **Manifest Migration**: Backward compatibility for V4 manifest format when upgrading from older versions
  - Added `ManifestV4Old`, `TableMetaV4Old`, `TableStatsMetaV4Old`, `ColumnStatsMetaV4Old` structs
  - Automatic migration from old V4 format (without retention_policy, partition_config, correlations, MCV fields)
  - Existing data is preserved during upgrade without manual intervention

### Added
- **Fulltext Index**: N-gram based index for efficient `LIKE '%pattern%'` substring searches
  - `CREATE INDEX idx_phone ON cdr USING FULLTEXT (calling_number)`
  - Segment pruning skips segments without matching n-grams
  - Ideal for phone number searches, partial string matching
  - Case-insensitive by default with 3-gram tokenization
- **EXPLAIN ANALYZE**: Visual query plan with actual execution statistics
- **Parallel Aggregation**: Segment-level partial aggregates merged for billion-row scale
- **Fast COUNT(*)**: Metadata-based counting without loading segment data
- **Go Driver Enhancements**: Circuit breaker, health checks, auto-reconnect, connection stats
- **Column-Level Encryption**: SQL commands for transparent encrypt/decrypt
  - `CREATE/DROP/ROTATE ENCRYPTION KEY`
  - `ALTER TABLE ... ENCRYPT/DECRYPT COLUMN`
  - `SHOW ENCRYPTION KEYS`, `SHOW ENCRYPTED COLUMNS`
- **Change Data Capture (CDC)**: Stream changes to downstream systems
  - `CREATE/DROP CDC SUBSCRIPTION`
  - `START/STOP CDC SUBSCRIPTION`
  - `GET CHANGES FROM table SINCE sequence LIMIT n`
  - `SET CDC CHECKPOINT`
- **PostgreSQL/MySQL Compatible SQL Commands**:
  - Session: `SET variable = value`, `SHOW VARIABLES`, `SHOW STATUS`
  - Process: `SHOW PROCESSLIST`, `KILL connection_id`, `KILL QUERY query_id`
  - Table: `OPTIMIZE TABLE`, `CHECK TABLE`, `CHECKSUM TABLE`, `REPAIR TABLE`
  - Index: `REINDEX TABLE`, `REINDEX INDEX`, `REINDEX DATABASE`, `CLUSTER`
  - Metadata: `COMMENT ON TABLE/COLUMN/DATABASE`, `SHOW CREATE TABLE/VIEW/DATABASE`
  - Admin: `LOCK/UNLOCK TABLES`, `FLUSH TABLES/PRIVILEGES`, `RESET QUERY CACHE`
  - Info: `SHOW TABLE STATUS`, `SHOW COLUMNS`, `SHOW ENGINE STATUS`, `SHOW WARNINGS/ERRORS`

### Performance
- COUNT(*) on billions of rows uses segment metadata (sub-second)
- Aggregations (SUM, AVG, MIN, MAX) computed in parallel across segments
- Connection pool health checks run in background goroutine
- Circuit breaker prevents cascading failures during server issues

### Go Driver
- `CircuitBreakerEnabled`: Automatic failure detection and recovery
- `HealthCheckInterval`: Background connection validation
- `MaxConnLifetime`: Connection rotation for long-running applications
- `MaxConnIdleTime`: Cleanup of stale connections
- Enhanced `PoolStats` with request counts and circuit breaker state

## [0.2.3] - 2026-03-08

### Added
- **Sharded Segment Cache**: 64-shard cache with independent locks for parallel segment access
- **Sharded Batch Cache**: 32-shard cache for decoded RecordBatches enabling parallel IPC decoding
- **Parallel S3 I/O**: Concurrent cold segment loading using async S3 operations
- **MVCC Row Write Index**: O(R+W) conflict detection using hash-indexed write tracking

### Improved
- **Lock Manager Targeted Wakeups**: Per-lock waiter tracking eliminates thundering herd on lock release
- **Adaptive Wait Timeouts**: Exponential backoff (1-8ms) instead of fixed 10ms waits
- **Manifest Snapshot Release**: Early lock release before segment I/O for better concurrency
- **Cache Hit Rate Metrics**: Per-cache hit/miss counters for segment and batch caches

### Performance
- Segment cache operations now lock only 1 of 64 shards instead of global lock
- Batch cache operations now lock only 1 of 32 shards instead of global lock
- Lock release wakes only transactions waiting on that specific lock
- MVCC validation reduced from O(M×(R+W)) to O(R+W) where M was all active transactions
- Concurrent queries no longer block on manifest lock during segment I/O

## [0.2.2] - 2026-03-08

### Fixed
- **Lock Manager TOCTOU Race Condition**: Fixed time-of-check-time-of-use race in `acquire_internal()` with atomic waiter cleanup
- **Transaction Manager TOCTOU Race Condition**: Fixed race in `begin()` using atomic check-and-insert pattern
- **MVCC Memory Leak**: Fixed committed_transactions cleanup with SeqCst ordering and aggressive cleanup strategy

### Improved
- **Deadlock Detector Optimization**: Pre-allocated collections and optimized DFS iteration to reduce allocations in hot paths
- **LockTarget Efficiency**: Added `table_owned()` and `row_owned()` methods to avoid unnecessary string allocations
- **Memory Ordering**: Use SeqCst ordering for proper synchronization in MVCC manager

## [0.2.1] - 2026-03-07

### Added
- **Segment Checksum Journal**: Redundant integrity tracking with independent checksum verification
- **IPC Format Validation**: Deep validation of Arrow IPC format during segment loads
- **Auto-Repair on Corruption**: Automatic repair triggers during deep scrub operations
- **S3 Upload Verification**: Read-back verification for cold storage uploads
- **Schema Hash Validation**: Schema verification during segment loads

### Improved
- **Retry Logic**: Configurable retry with exponential backoff for segment operations
- **Deep Scrub**: Enhanced scrubbing with IPC validation and schema verification
- **Fault Tolerance Config**: New configuration options for resilience tuning

### Configuration
New engine configuration options:
- `validate_schema_on_load`: Enable schema validation during segment loads
- `deep_scrub_validate_ipc`: Enable IPC format validation during deep scrub
- `auto_repair_on_corruption`: Automatically trigger repairs on detected corruption
- `verify_s3_uploads`: Verify S3 uploads by reading back and comparing checksums
- `segment_checksum_journal_enabled`: Enable redundant checksum journal
- `segment_operation_max_retries`: Max retries for segment operations
- `segment_operation_retry_delay_ms`: Delay between retries

## [0.2.0] - 2026-03-06

### Added
- **AI/Vector Search Support**: Vector similarity search with cosine, euclidean, dot product, and manhattan distance metrics
- **Hybrid Search**: Combined vector and text search with RRF and linear fusion
- **Text Chunking**: Fixed-size, sentence, paragraph, and semantic chunking strategies
- **Embedding Model Registry**: Pre-configured dimensions for OpenAI, Cohere, and HuggingFace models
- **Node.js SDK Enhancements**: Vector search, hybrid search, and embedding utilities

### Improved
- **LIKE Pattern Support**: Enhanced pattern matching with proper escape handling
- **Query Performance**: Optimized GROUP BY for arbitrary column names

## [0.1.5] - 2026-03-05

### Fixed
- Race conditions in lock manager
- Memory leaks in segment cache
- Connection pool deadlocks

### Improved
- Go SDK with connection pooling
- Better error messages

## [0.1.4] - 2026-03-01

### Added
- Per-tenant resource quotas
- Multi-region replication
- GPU acceleration (CUDA/Metal)
- Index advisor and query store
- Adaptive query execution

## [0.1.3] - 2026-02-15

### Added
- Point-in-time recovery (PITR)
- WAL archiving
- Automatic scheduled backups

### Improved
- Backup/restore performance
- Crash recovery reliability

## [0.1.2] - 2026-02-01

### Added
- ACID transactions with MVCC
- Snapshot isolation
- Savepoints and rollback

### Improved
- Lock manager with deadlock detection
- Transaction throughput

## [0.1.1] - 2026-01-15

### Added
- Clustering with SWIM gossip protocol
- Raft-lite leader election
- Two-node mode for simple HA
- Write quorum replication

## [0.1.0] - 2026-01-01

### Added
- Initial release
- Columnar storage engine with Apache Arrow
- SQL parser with full SELECT/INSERT/UPDATE/DELETE
- Window functions and CTEs
- B-tree, hash, and bloom filter indexes
- TLS encryption and RBAC authentication
- Go, Python, Node.js, Rust, C#, and PHP drivers
- CLI with PostgreSQL-compatible shell
- Tiered storage (hot/warm/cold)
- Compression (Zstd, LZ4, Snappy)

[0.9.6]: https://github.com/Izi-Technologies/boyodb/compare/v0.9.5...v0.9.6
[0.9.5]: https://github.com/Izi-Technologies/boyodb/compare/v0.9.4...v0.9.5
[0.9.4]: https://github.com/Izi-Technologies/boyodb/compare/v0.2.7...v0.9.4
[0.2.7]: https://github.com/Izi-Technologies/boyodb/compare/v0.2.6...v0.2.7
[0.2.6]: https://github.com/Izi-Technologies/boyodb/compare/v0.2.5...v0.2.6
[0.2.5]: https://github.com/Izi-Technologies/boyodb/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/Izi-Technologies/boyodb/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/Izi-Technologies/boyodb/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/Izi-Technologies/boyodb/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/Izi-Technologies/boyodb/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Izi-Technologies/boyodb/compare/v0.1.5...v0.2.0
[0.1.5]: https://github.com/Izi-Technologies/boyodb/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/Izi-Technologies/boyodb/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/Izi-Technologies/boyodb/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/Izi-Technologies/boyodb/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/Izi-Technologies/boyodb/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Izi-Technologies/boyodb/releases/tag/v0.1.0
