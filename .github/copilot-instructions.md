# DRDB Copilot Instructions

## Project Overview

**DRDB** (Data Replication Database) is a high-performance, low-latency columnar database engine with:
- **Rust core** (`boyodb-core`): Append-only columnar storage with Arrow IPC, C ABI, cluster HA
- **Multi-language bindings**: Go (cgo), Node.js (napi-rs), Python, C#, PHP drivers
- **Server**: TCP/TLS with JSON protocol and cluster support (`boyodb-server`)
- **Target workloads**: Analytics, time-series, high-throughput ingest

## Critical Architecture

### Core Engine (`crates/boyodb-core`)

The engine is a **stateful, single-writer columnar database** with these layers:

1. **FFI Surface** (`src/ffi.rs`): C ABI exports via `#[no_mangle] extern "C"` functions
   - `boyodb_open/close`: Handle lifecycle
   - `boyodb_ingest_ipc*` (v1/v2/v3): Arrow IPC batch ingestion with optional shard hints
   - `boyodb_query_ipc`: SQL query execution returning Arrow IPC batches
   - `boyodb_plan_bundle`: Replication manifest bundling with bandwidth caps

2. **Engine** (`src/engine.rs`, ~10K LOC): Core query/ingest orchestration
   - Parses SQL → `ParsedQuery` via `sqlparser` crate
   - Ingest: Write-Ahead Log (WAL) + segment file + manifest atomic update
   - Query: Manifest-driven segment scanning with optional bloom filters for predicates
   - Aggregations: COUNT(*), SUM, AVG, MIN, MAX with GROUP BY (tenant_id, route_id)
   - Segments: CRC32 checksummed, optional zstd compression, stored in `data_dir/segments/*.ipc`

3. **SQL Parser** (`src/sql.rs`): Converts text to `ParsedQuery`/`SqlStatement`
   - Supports: SELECT (projection, WHERE, ORDER BY, LIMIT, DISTINCT), GROUP BY, JOINs, WITH CTEs
   - Filters extracted into `QueryFilter` struct (watermark, event_time, tenant_id, route_id ranges)
   - DDL commands: CREATE/DROP DATABASE/TABLE, ALTER TABLE, TRUNCATE

4. **Replication** (`src/replication.rs`): Manifest versioning and bundle planning
   - `Manifest`: Version counter + list of `ManifestEntry` (segment metadata)
   - Persists to `data_dir/manifest.json` (snapshot: `wal/manifest.snapshot.json`)
   - `BundleRequest`: Selects segments with `max_bytes`, `prefer_hot`, `since_version` filters
   - Segment Tiers: Hot/Warm/Cold for lifecycle management

5. **WAL** (`src/wal.rs`): Append-only log for crash recovery
   - Stores serialized `WalRecord::Segment` entries
   - Rotates when exceeding `wal_max_bytes` (default 64MB)
   - Replay on startup re-materializes missing segments/manifest

6. **Cluster HA** (`src/cluster/`): Gossip + leader election + write replication
   - Gossip protocol (UDP, SWIM): Node discovery and health monitoring
   - Election (Raft-lite): Lease-based leader election
   - Replication coordinator: Quorum-based write durability across nodes

### Crate Structure

- `boyodb-core`: Core lib (cdylib + rlib) — shared Rust + C consumers
- `boyodb-server`: TCP server with JSON protocol (binaries in `target/release`)
- `boyodb-cli`: Minimal CLI wrapper around C ABI or server connection
- `bindings/go`, `bindings/node`: Multi-language FFI wrappers
- `drivers/`: Standalone clients (Python, Go, C#, PHP, Node.js, Rust)

## Build & Test Workflows

### Build (via Makefile)

```bash
make build-core           # cargo build -p boyodb-core --release
make build-node          # Rust → Node.js binding via napi-rs
make build-go            # cgo wrapper (requires build-core first)
make build-all           # All three
make clean
```

**Key**: Release builds enable optimizations: `opt-level=3`, `lto="thin"`, `codegen-units=1`, `strip=true`.

### Test Patterns

- **Unit tests**: Within crate source files (e.g., `#[test]` in `engine.rs`)
- **Integration tests**: `crates/boyodb-server/tests/integration_tests.rs`
  - Uses tempfile for isolated data_dir
  - Creates Arrow IPC test data via `arrow_array` builders
  - Tests server via `send_request()`/framed JSON protocol (4-byte BE length + JSON)
  - **Run**: `cargo test --package boyodb-server -- --ignored` (marked `#[ignore]`)
- **Manual testing**: Use CLI tools:
  ```bash
  cargo run -p boyodb-cli -- shell --host localhost:8765
  cargo run -p boyodb-server /tmp/boyodb 0.0.0.0:8765
  ```

## Key Developer Patterns

### 1. FFI Memory Management

C ABI functions use `OwnedBuffer` (defined `src/types.rs`) for safe zero-copy hand-off:
```rust
pub struct OwnedBuffer {
    pub data: *mut u8,
    pub len: usize,
    pub capacity: usize,
    pub destructor: Option<extern "C" fn(*mut c_void, *mut u8, usize, usize)>,
    pub destructor_state: *mut c_void,
}
impl OwnedBuffer {
    pub fn from_vec(v: Vec<u8>) -> Self { /* drops Vec into raw parts, transfers ownership */ }
    extern "C" fn drop_vec(/**/){ /* reconstruct Vec from raw parts and drop */ }
}
```
**Pattern**: All query results, segment data, and manifest exports use `OwnedBuffer` — never allocate C-side.

### 2. Arrow IPC Format

- **Input**: Ingest expects `arrow_ipc::writer::StreamReader` format (batch frames)
- **Output**: Query results and segments stored as `arrow_ipc::writer::StreamWriter` frames
- **No schema versioning per segment** — all columns must match table schema from manifest
- **Use case**: Columnar analytics + zero-copy hand-off via Arrow C Data Interface (future)

### 3. Manifest-Driven Design

Every ingest is **atomic**:
1. Append to WAL
2. Write segment IPC to `data_dir/segments/<segment_id>.ipc`
3. Update manifest.json + compute CRC32
4. On read: consult manifest to discover which segments match query (bloom filters for tenant_id, route_id)

**Consequence**: Concurrent readers see consistent view; no read locks needed on engine.

### 4. Shard Hints for Parallel Ingest

Three ingest variants handle different metadata:
- `boyodb_ingest_ipc` (v1): Auto-assign shard
- `boyodb_ingest_ipc_v2`: Caller provides shard_id hint
- `boyodb_ingest_ipc_v3`: Caller provides shard_id + database/table metadata

**Use v3 when possible** — keeps catalog aligned without separate DDL calls.

### 5. Aggregation & Grouping

Supported aggregations on columnar data:
```rust
enum AggKind { CountStar, Sum{column}, Avg{column}, Min{column}, Max{column} }
```
- **Grouped** (`GROUP BY tenant_id | route_id`): COUNT, SUM, AVG per group
- **Global** (no GROUP BY): COUNT, SUM, AVG, MIN, MAX across all rows
- MIN/MAX are **global-only** currently (no GROUP BY support)

Results returned as Arrow IPC batches with aggregated columns.

### 6. SQL Query Filter Extraction

Filters in WHERE clause are pre-parsed into `QueryFilter` struct to enable manifest-level pruning:
```rust
pub struct QueryFilter {
    pub watermark_ge/le: Option<u64>,
    pub event_time_ge/le: Option<u64>,
    pub tenant_id_eq: Option<u64>,
    pub route_id_eq: Option<u64>,
    pub like_filters: Vec<(column, pattern, negate)>,
    pub null_filters: Vec<(column, is_null)>,
    pub string_eq_filters: Vec<(column, value)>,
    pub string_in_filters: Vec<(column, Vec<value>)>,
}
```
**Impact**: Bloom filters in manifest metadata allow segment skipping without reading data.

### 7. Error Handling

FFI returns `CboyodbStatus` enum (C-compatible):
```rust
pub enum CboyodbStatus {
    Ok = 0, InvalidArgument = 1, NotFound = 2, Internal = 3,
    NotImplemented = 4, Io = 5, Timeout = 6,
}
impl CboyodbStatus::from_result(Result<T, EngineError>) -> CboyodbStatus
```
**Pattern**: Rust-side errors converted to status codes; messages logged via tracing not exported.

## Integration Points & External Dependencies

- **Arrow**: `arrow_array`, `arrow_schema`, `arrow_ipc` — columnar format + IPC serialization
- **sqlparser**: SQL lexing/parsing (GenericDialect)
- **zstd**: Optional compression for segments
- **bloom**: Bloom filters for predicate pushdown on manifest metadata
- **tokio**: Async runtime (server, cluster gossip/replication)
- **tracing**: Structured logging (enabled in release via `opt-level=3`)
- **parking_lot**: Fast Mutex/RwLock (used in engine for segment cache)
- **rayon**: Data-parallel operations (aggregations, joins)

## Multi-Language Binding Conventions

### Go (`bindings/go/boyodb`)
- **Pattern**: cgo wrapper → C ABI → FFI functions
- **Linker**: Defaults to `target/release`; `#cgo LDFLAGS: -L...`
- **Lifetime**: `Open()` returns handle, `defer h.Close()`

### Node.js (`bindings/node`)
- **Pattern**: napi-rs Rust → Node native module (`.node` binary)
- **Build**: `cargo build -p boyodb-node --release` generates `index.node`
- **Exports**: `Database` class with async methods (`ingest`, `query`, `planBundle`)

### Python, C#, PHP (`drivers/`)
- Standalone implementations (not FFI wrappers)
- May use TCP client to `boyodb-server` instead of C ABI

## Server Protocol (boyodb-server)

**TCP/TLS with framed JSON**:
- Request: 4-byte BE length prefix + JSON body
- Response: Same format
- Auth: Token-based or user/password (configurable)
- Commands: Same as SQL queries + cluster gossip/replication messages

**Key CLI usage**:
```bash
cargo run -p boyodb-server -- /data/dir 0.0.0.0:8765 \
  --cluster --cluster-id my-cluster \
  --gossip-addr 0.0.0.0:8766 \
  --seed-nodes "10.0.0.1:8766,10.0.0.2:8766"
```

## Debugging Tips

1. **Enable tracing**: Set `RUST_LOG=debug` env var before running
2. **WAL recovery**: Check `data_dir/wal/` for crash-recovery journal
3. **Manifest inspection**: `data_dir/manifest.json` is human-readable JSON
4. **Segment files**: `data_dir/segments/*.ipc` are Arrow streaming format (inspect with `arrow inspect`)
5. **Test with TempDir**: Use `tempfile::TempDir` for isolated test environments (auto-cleanup)

## Common Workflows

### Adding a Query Feature
1. Extend `sqlparser::ast::Select` parsing in `sql.rs` → `ParsedQuery`
2. Add field to `QueryFilter` for manifest-level pruning (optional)
3. Implement filter logic in `engine.rs` within scan loop
4. Return results as Arrow RecordBatch via `StreamWriter`

### Adding an Aggregation
1. Add variant to `AggKind` enum in `sql.rs`
2. Extend parser to recognize function in aggregate position
3. Implement accumulator in `engine.rs` (scan + grouping loop)
4. Write test in integration_tests.rs

### Adding a Cluster Feature
1. Define new message type in `cluster/messages.rs`
2. Add handler in `cluster/replication.rs` or `gossip.rs`
3. Test with multi-node setup using TempDir for isolated data_dirs
4. Use `RUST_LOG=debug` to trace gossip/election messages

### Debugging FFI Crashes
1. Call Rust-side from language binding with minimal test case
2. Add `eprintln!` or tracing in FFI layer before pointer dereference
3. Enable debug symbols: Use `[profile.release] debug = true` in Cargo.toml
4. Run under debugger (lldb on macOS): `lldb -- /path/to/test`
