# Changelog

All notable changes to BoyoDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.4] - 2026-03-08

### Fixed
- **Manifest Migration**: Backward compatibility for V4 manifest format when upgrading from older versions
  - Added `ManifestV4Old`, `TableMetaV4Old`, `TableStatsMetaV4Old`, `ColumnStatsMetaV4Old` structs
  - Automatic migration from old V4 format (without retention_policy, partition_config, correlations, MCV fields)
  - Existing data is preserved during upgrade without manual intervention

### Added
- **Fulltext Index**: N-gram based index for efficient `LIKE '%pattern%'` substring searches
  - `CREATE INDEX idx_phone ON cdr (calling_number) USING FULLTEXT`
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

[0.2.2]: https://github.com/Izi-Technologies/boyodb/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/Izi-Technologies/boyodb/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Izi-Technologies/boyodb/compare/v0.1.5...v0.2.0
[0.1.5]: https://github.com/Izi-Technologies/boyodb/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/Izi-Technologies/boyodb/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/Izi-Technologies/boyodb/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/Izi-Technologies/boyodb/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/Izi-Technologies/boyodb/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Izi-Technologies/boyodb/releases/tag/v0.1.0
