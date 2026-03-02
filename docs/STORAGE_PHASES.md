# Storage Phases Roadmap

This document captures the planned storage phases for achieving fast write
throughput and low read latency for production analytics workloads.

## Phase 1: Fast Writes + Stable Reads

Status: complete

1) Memtable + WAL group commit
   - Buffer writes in memory.
   - Append WAL for durability.
   - Flush in batches with a small group-commit window (e.g., 1-5 ms).

2) Sorted hot segments
   - Flush into small sorted columnar segments.
   - Default sort key: event_time + tenant/route.
   - Improves data skipping and read latency.

3) Per-segment stats
   - Min/max, null count, bloom per column.
   - Enables fast segment pruning.

## Phase 2: Read Latency Wins

4) Aggressive background compaction
   - Merge small hot segments into larger sorted segments.
   - Keep hot tier lightly compressed; compress warm/cold more aggressively.

5) Projection + filter pushdown everywhere
   - Pushdown in scans, joins, aggregations, and IPC filtering.

6) Hot metadata + block cache
   - Cache manifest, column stats, and frequently accessed column blocks.

## Phase 3: Concurrency + Scale

7) MVCC snapshot reads
   - Readers never block writers.
   - Consistent snapshots during writes.

8) Async secondary indexes
   - Optional LSM-style indexes for selective filters.
   - Built and maintained in the background.

## Phase 4: Operational Parity

9) Checkpointing + WAL trimming
   - Control WAL growth and recovery time.

10) Backup/restore snapshots
    - Consistent snapshots for restore and replication.

11) Replication modes
    - Logical and/or physical replication, depending on latency goals.

## Phase 5: Advanced Columnar Optimization

12) Column encodings
    - Dictionary, delta, RLE, and others per column.

13) Index granularity tuning
    - Configurable granularity for pruning tradeoffs.

14) Tiered storage
    - Hot, warm, cold tiers with different compression and caching policies.
