# BoyoDB Future Roadmap

To evolve BoyoDB into a general-purpose Big Data engine capable of Petabyte-scale analytics, we propose the following strategic enhancements.

## 1. Distributed Query Engine (Sharding)
**Status**: Implemented (Phase 2 Completed)
**Priority**: High

Currently, `cluster` supports HA but not distributed query execution.
*   **Goal**: Allow queries to span hundreds of nodes.
*   **Design**:
    *   **Coordinator Node**: Parses SQL, plans query, and scatters sub-queries to shards.
    *   **Data Nodes**: Execute local aggregation/filtering on shards.
    *   **Scatter-Gather**: Parallel merge of results at the coordinator.
    *   **Partitioning**: Hash-based sharding by primary key (e.g., `tenant_id`).

## 2. Cloud-Native Tiering (S3/Object Store)
**Status**: Implemented (Phase 1 & Lifecycle)
**Priority**: High

**Goal**: Unlimited retention at low cost.
*   **Design**:
    *   **Hot Tier**: NVMe (current).
    *   **Cold Tier**: S3/GCS/Azure Blob.
    *   **Lifecycle Policy**: Auto-move segments to S3 after N days.
    *   **Range Requests**: Query engine reads only required column chunks from S3, avoiding full-file downloads.

## 3. Advanced Analytics & Algorithms
**Status**: Partially Implemented (HLL Completed)
**Priority**: Medium

**Goal**: Support complex analytical queries efficiently.
*   **HyperLogLog (HLL)**:
    *   `APPROX_COUNT_DISTINCT(col)` for massive cardinality estimation with constant memory.
    *   Crucial for "User Unique" counts in ad-tech/analytics.
*   **Window Functions**: `ROW_NUMBER()`, `RANK()`, `LEAD/LAG` for time-series analysis.
*   **T-Digest**: For accurate percentile (p99, p95) approximations.

## 4. Ecosystem Integration
**Status**: Planned
**Priority**: Medium

**Goal**: Seamless integration with existing data tools.
*   **PostgreSQL Wire Protocol**:
    *   **Status**: In Progress (Phase 1 Complete)
    *   Implement `pgwire` server side.
    *   Allows connecting via `psql`, Tableau, Looker, Grafana without custom drivers.
*   **Kafka Connect Sink**:
    *   Native connector to stream Kafka topics directly into BoyoDB ingestion API.

## 5. Security & Governance
**Status**: Planned
**Priority**: Low

*   **RBAC V2**: Fine-grained column-level permissioning.
*   **Audit Logging**: tamper-evident query logs.
