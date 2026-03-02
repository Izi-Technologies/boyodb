# Getting Started with BoyoDB

Welcome to BoyoDB! This guide will walk you through installing, configuring, and using BoyoDB, a high-performance columnar database.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Running the Server](#running-the-server)
4. [Connecting with the Shell](#connecting-with-the-shell)
5. [Basic Operations](#basic-operations)
6. [Next Steps](#next-steps)

---

## Prerequisites

Before you begin, ensure you have the following installed:

- **Rust Toolchain** (1.75+): Required to build from source.
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```
- **Make**: For running build automation scripts.
- **GCC/Clang**: Required for building C FFI bindings.

---

## Installation

BoyoDB is currently distributed as source code. You can build it easily using `cargo` or `make`.

### 1. Clone the Repository

```bash
git clone https://github.com/loreste/boyodb.git
cd boyodb
```

### 2. Build the Project

You can build all components (server, CLI, engine) with a single command:

```bash
make build-all
```

Alternatively, build specific components using `cargo`:

```bash
# Build the server (release mode recommended for performance)
cargo build -p boyodb-server --release

# Build the CLI tool
cargo build -p boyodb-cli --release
```

The compiled binaries will be located in `target/release/`.

---

## Running the Server

### 1. Create a Data Directory
BoyoDB needs a directory to store its data (segments, manifest, WAL).

```bash
mkdir -p ./data
```

### 2. Start the Server
Run the server pointing to your data directory. By default, it listens on `127.0.0.1:8765`.

```bash
# Using cargo
cargo run -p boyodb-server --release -- ./data 127.0.0.1:8765

# OR using the compiled binary directly
./target/release/boyodb-server ./data 127.0.0.1:8765
```

You should see logs indicating the server has started:
```text
INFO boyodb_server: boyodb-server started bind_addr=127.0.0.1:8765 ...
```

---

## Connecting with the Shell

BoyoDB provides an interactive SQL shell (`bsql`) for managing the database.

### 1. Using the `bsql` script
We provide a convenience wrapper script:

```bash
./scripts/bsql
```

### 2. Using `boyodb-cli` directly
```bash
./target/release/boyodb-cli shell --host 127.0.0.1:8765
```

You will see the shell prompt:
```text
boyodb>
```

---

## Basic Operations

Here is a quick tour of standard database operations.

### 1. Create a Database
```sql
boyodb> CREATE DATABASE analytics;
Database created
```

### 2. Switch Context
Switch to the new database to run queries against it.
```sql
boyodb> \c analytics
Switched to database: analytics
```

### 3. Create a Table
BoyoDB is strongly typed. Create a table to store events.
```sql
boyodb[analytics]> CREATE TABLE events (
    id INT64,
    event_type STRING,
    event_time TIMESTAMP
);
Table created
```

### 4. Insert Data
Insert a few rows.
```sql
boyodb[analytics]> INSERT INTO events (id, event_type, event_time) VALUES
    (1, 'login', NOW()),
    (2, 'view_page', NOW()),
    (3, 'logout', NOW());
3 rows inserted
```

### 5. Query Data
Run a standard SQL query.
```sql
boyodb[analytics]> SELECT * FROM events;
┌────┬────────────┬──────────────────┐
│ id │ event_type │ event_time       │
├────┼────────────┼──────────────────┤
│ 1  │ login      │ 1704484000000000 │
│ 2  │ view_page  │ 1704484000000000 │
│ 3  │ logout     │ 1704484000000000 │
└────┴────────────┴──────────────────┘
3 row(s)
```

### 6. Inspect Schema
Use meta-commands to look at your schema.
```sql
boyodb[analytics]> \dt
Tables:
  events

boyodb[analytics]> \d events
Table: analytics.events
Columns:
Name                 Type            Nullable
--------------------------------------------------
id                   Int64           YES
event_type           String          YES
event_time           Timestamp       YES
```

---

## Next Steps

Now that you have BoyoDB running, explore these resources to learn more:

- **[Interactive Shell Guide](SHELL.md)**: Master the CLI, including history, formatting, and file I/O.
- **[SQL Reference](SQL.md)**: Full documentation of supported SQL syntax and functions.
- **[Language Drivers](CLI.md#client-drivers)**: Connect from Go, Node.js, Python, or Rust.
- **[API Reference](API.md)**: Detailed TCP/TLS wire protocol documentation.
- **[Security Guide](SECURITY.md)**: Production hardening, authentication, and TLS setup.

---

## Production & Performance Tuning

For high-volume "Big Data" workloads, default settings may bottleneck performance. Follow these guidelines for optimal throughput:

### 1. Build for Release
Always use the `--release` flag. Debug builds are significantly slower (up to 10-50x).
```bash
cargo build --release
```
See [PERF_BASELINE.md](../PERF_BASELINE.md) for expected benchmark numbers on standard hardware.

### 2. Batch Ingestion
Avoid single-row `INSERT` statements for bulk data. BoyoDB is optimized for **Arrow IPC** ingestion.
- Use `boyodb-cli ingest` or language driver batch methods.
- **Recommended Batch Size**: 50,000 - 100,000 rows (aim for 64KB-256KB per column chunk) to balance throughput and memory/WAL overhead.

### 3. Hardware Configuration
- **Storage**: Use **NVMe/SSD** for the `--data-dir`. Columnar seeking relies on fast random read performance.
- **WAL Isolation**: Place the Write-Ahead Log on a separate physical device using `--wal-dir` to prevent ingest writes from contending with query reads/flushes.
- **Memory**: 16GB+ RAM is recommended for high throughput. Use the **High Memory** profile settings below.
- **CPU**: BoyoDB scales linearly with cores for parallel query scanning. Use `--workers=$(nproc)` (default).

### 4. Memory & Caching (High Memory Profile)
For servers with 16GB+ RAM, use these aggressive defaults to maximize throughput:
- **Batch Cache**: Increase to 4GB (`--batch-cache-bytes 4294967296`) to absorb write bursts.
- **Query Cache**: Increase to 4GB (`--query-cache-bytes 4294967296`) for analytical workloads.
- **IPC Limits**: Increase `--max-ipc-bytes` to 512MB (`--max-ipc-bytes 536870912`) for massive batch ingestion.

### 5. Strict Durability (High Assurance)
By default, BoyoDB uses strict per-write `fsync`. Ensure your configuration maintains this for zero-data-loss guarantees:
- `--wal-sync-bytes 0`: Forces an fsync after *every* write payload.
- `--wal-sync-interval-ms 0`: Forces an immediate fsync without delay.

> **Note**: Strict fsync requires NVMe storage to maintain high ingestion rates (500k+ rows/s). On spinning disks, relaxation may be necessary (`--wal-sync-bytes 65536`).

### 5. Schema Design
- **Partitioning**: Use sensible shard keys or partition predicates (like `tenant_id` or `event_time`) in your queries to allow the engine to prune segments effectively.
- **Sorting**: Data is sorted by `event_time` + `route_id` by default. Align your queries with this sort order for maximum performance.

