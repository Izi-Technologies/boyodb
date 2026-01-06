# Server API Reference

Complete API reference for the BoyoDB TCP/TLS server.

## Protocol Overview

### Connection

BoyoDB uses a length-prefixed JSON protocol over TCP or TLS:

```
┌────────────────┬────────────────────────────────────┐
│ Length (4B BE) │ JSON Payload (UTF-8)               │
└────────────────┴────────────────────────────────────┘
```

- **Length**: 4-byte big-endian unsigned integer
- **Payload**: UTF-8 encoded JSON object

### Authentication

If the server is started with `--token`, include it in every request:

```json
{"auth": "your-secret-token", "op": "health"}
```

For user authentication, first call `login`:

```json
{"op": "login", "username": "admin", "password": "secret"}
```

---

## Operations

### Health Check

Check server status.

**Request:**
```json
{"op": "health"}
```

**Response:**
```json
{
  "status": "ok",
  "message": "healthy"
}
```

### Detailed Health

Get detailed server metrics.

**Request:**
```json
{"op": "health_detailed"}
```

**Response:**
```json
{
  "status": "ok",
  "health": {
    "uptime_secs": 3600,
    "databases": 5,
    "tables": 12,
    "segments": 1234,
    "total_bytes": 1073741824,
    "queries_total": 50000,
    "ingests_total": 10000
  }
}
```

---

## Query Operations

### Query (JSON Response)

Execute a SQL query with JSON-encoded results.

**Request:**
```json
{
  "op": "query",
  "sql": "SELECT * FROM mydb.users LIMIT 10",
  "timeout_millis": 30000,
  "database": "mydb",
  "accept_compression": "zstd"
}
```

**Parameters:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sql` | string | Yes | SQL query |
| `timeout_millis` | integer | No | Query timeout (default: 10000) |
| `database` | string | No | Default database for unqualified tables |
| `accept_compression` | string | No | Request compressed response ("zstd") |

**Response:**
```json
{
  "status": "ok",
  "ipc_base64": "QVJST1cxAAA...",
  "segments_scanned": 5,
  "data_skipped_bytes": 102400,
  "compression": "zstd"
}
```

### Query Binary

Execute query with binary IPC response (for prepared statements).

**Request:**
```json
{
  "op": "query_binary",
  "sql": "EXECUTE my_statement",
  "timeout_millis": 30000,
  "database": "mydb",
  "stream": true
}
```

**Response:** JSON header frame followed by one or more binary IPC frames. When `stream=true`,
the header includes `ipc_streaming: true` and the payload is sent as chunked frames ending with
an empty frame.

### Explain

Get query execution plan without executing.

**Request:**
```json
{
  "op": "explain",
  "sql": "SELECT * FROM mydb.users WHERE id = 42"
}
```

**Response:**
```json
{
  "status": "ok",
  "explain_plan": {
    "database": "mydb",
    "table": "users",
    "projection": ["*"],
    "filters": ["id = 42"],
    "segments_to_scan": 3,
    "total_bytes": 1048576,
    "uses_parallel_scan": true,
    "uses_bloom_filter": true
  }
}
```

---

## Data Ingestion

### Ingest Arrow IPC

Ingest data in Arrow IPC format (base64 encoded).

**Request:**
```json
{
  "op": "ingest_ipc",
  "payload_base64": "QVJST1cxAAA...",
  "watermark_micros": 1705312800000000,
  "database": "mydb",
  "table": "events",
  "shard_hint": 42,
  "compression": "zstd"
}
```

**Parameters:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `payload_base64` | string | Yes | Base64-encoded Arrow IPC |
| `watermark_micros` | integer | Yes | Watermark timestamp (microseconds) |
| `database` | string | No | Target database |
| `table` | string | No | Target table |
| `shard_hint` | integer | No | Shard routing hint |
| `compression` | string | No | Payload compression ("zstd") |

**Response:**
```json
{
  "status": "ok",
  "message": "ingest ok",
  "rows_ingested": 1000,
  "segment_id": 42
}
```

### Ingest Arrow IPC Binary

Ingest binary Arrow IPC (not base64 encoded).

**Request:**
```json
{
  "op": "ingest_ipc_binary",
  "watermark_micros": 1705312800000000,
  "database": "mydb",
  "table": "events"
}
```

Followed by binary Arrow IPC payload.

### Ingest CSV

Ingest CSV data.

**Request:**
```json
{
  "op": "ingest_csv",
  "payload_base64": "aWQsbmFtZQoxLEpvaG4KMixKYW5l",
  "database": "mydb",
  "table": "users",
  "schema": [
    {"name": "id", "type": "int64", "encoding": "delta"},
    {"name": "name", "type": "string", "encoding": "dictionary"}
  ],
  "has_header": true,
  "delimiter": ",",
  "watermark_micros": 1705312800000000,
  "infer_schema_rows": 1024
}
```

**Parameters:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `payload_base64` | string | Yes | Base64-encoded CSV |
| `database` | string | Yes | Target database |
| `table` | string | Yes | Target table |
| `schema` | array | No | Column definitions |
| `has_header` | boolean | No | CSV has header row (default: true) |
| `delimiter` | string | No | Field delimiter (default: ",") |
| `watermark_micros` | integer | No | Watermark timestamp |
| `infer_schema_rows` | integer | No | Rows to scan for schema inference |

**Response:**
```json
{
  "status": "ok",
  "message": "csv ingest ok",
  "rows_ingested": 100
}
```

---

## Database Operations

### List Databases

**Request:**
```json
{"op": "list_databases"}
```

**Response:**
```json
{
  "status": "ok",
  "databases": ["analytics", "production", "staging"]
}
```

### Create Database

**Request:**
```json
{
  "op": "create_database",
  "name": "analytics"
}
```

**Response:**
```json
{
  "status": "ok",
  "message": "database created"
}
```

---

## Table Operations

### List Tables

**Request:**
```json
{
  "op": "list_tables",
  "database": "analytics"
}
```

**Response:**
```json
{
  "status": "ok",
  "tables": [
    {
      "database": "analytics",
      "name": "events",
      "schema_json": "{\"fields\":[...]}",
      "compression": "zstd"
    },
    {
      "database": "analytics",
      "name": "users",
      "schema_json": null
    }
  ]
}
```

### Create Table

**Request:**
```json
{
  "op": "create_table",
  "database": "analytics",
  "table": "events",
  "schema": [
    {"name": "id", "type": "int64", "nullable": false},
    {"name": "event_type", "type": "string", "encoding": "dictionary"},
    {"name": "timestamp", "type": "uint64", "encoding": "delta"}
  ],
  "compression": "zstd"
}
```

**Response:**
```json
{
  "status": "ok",
  "message": "table created"
}
```

Schema fields support an optional `encoding` value: `dictionary`, `delta`, or `rle`.
`delta` is supported for `int64` and `timestamp` columns.

---

## Manifest Operations

### Get Manifest

**Request:**
```json
{"op": "manifest"}
```

**Response:**
```json
{
  "status": "ok",
  "manifest_json": {
    "format_version": 1,
    "version": 42,
    "databases": ["analytics"],
    "tables": [...],
    "entries": [...]
  }
}
```

### Apply Manifest

Import a manifest (requires `--allow-manifest-import`).

**Request:**
```json
{
  "op": "apply_manifest",
  "manifest_json": {...},
  "overwrite": false
}
```

### Plan Bundle

Plan a replication bundle.

**Request:**
```json
{
  "op": "plan_bundle",
  "max_bytes": 1048576,
  "since_version": 10,
  "prefer_hot": true,
  "target_bytes_per_sec": 200000
}
```

**Response:**
```json
{
  "status": "ok",
  "bundle_plan": {
    "entries": [...],
    "total_bytes": 524288,
    "throttle_millis": 2621
  }
}
```

### Export Bundle

Export manifest and segment data for replication.

**Request:**
```json
{
  "op": "export_bundle",
  "max_bytes": 1048576,
  "since_version": 10,
  "prefer_hot": true,
  "max_entries": 128
}
```

**Response:**
```json
{
  "status": "ok",
  "bundle_json": {
    "manifest_slice": {...},
    "segments": [...]
  }
}
```

### Apply Bundle

Apply an exported bundle.

**Request:**
```json
{
  "op": "apply_bundle",
  "bundle_json": {...}
}
```

---

## Maintenance Operations

### Compact

Merge segments for a table.

**Request:**
```json
{
  "op": "compact",
  "database": "analytics",
  "table": "events"
}
```

**Response:**
```json
{
  "status": "ok",
  "message": "compacted table analytics.events: merged 5 segments"
}
```

### Checkpoint

Flush memtables + manifest and truncate WAL segments (superuser only).

**Request:**
```json
{
  "op": "checkpoint"
}
```

**Response:**
```json
{
  "status": "ok",
  "message": "checkpoint ok"
}
```

### WAL Stats

Get WAL trimming stats (superuser only).

**Request:**
```json
{
  "op": "wal_stats"
}
```

**Response:**
```json
{
  "status": "ok",
  "wal_stats": {
    "wal_dir": "/var/lib/boyodb/wal",
    "wal_log_bytes": 0,
    "wal_log_modified_millis": 1710000000000,
    "rotated_count": 2,
    "rotated_bytes": 8192,
    "rotated_oldest_millis": 1710000000000,
    "rotated_newest_millis": 1710000005000,
    "wal_max_bytes": 67108864,
    "wal_max_segments": 4,
    "rotated_trim_needed": 0
  }
}
```

### Insight

Get table statistics from manifest.

**Request:**
```json
{
  "op": "insight",
  "database": "analytics",
  "table": "events"
}
```

**Response:**
```json
{
  "status": "ok",
  "insight": {
    "database": "analytics",
    "table": "events",
    "segment_count": 12,
    "total_rows": 1000000,
    "total_bytes": 52428800,
    "min_watermark": 1700000000000000,
    "max_watermark": 1705312800000000
  }
}
```

### Metrics

Get server metrics.

**Request:**
```json
{"op": "metrics"}
```

**Response:**
```json
{
  "status": "ok",
  "metrics": {
    "queries_total": 50000,
    "ingests_total": 10000,
    "bytes_ingested": 1073741824,
    "cache_hits": 45000,
    "cache_misses": 5000
  }
}
```

---

## Authentication Operations

### Login

Authenticate with username/password.

**Request:**
```json
{
  "op": "login",
  "username": "admin",
  "password": "secret123"
}
```

**Response:**
```json
{
  "status": "ok",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "message": "logged in as admin"
}
```

### Logout

End the current session.

**Request:**
```json
{"op": "logout"}
```

**Response:**
```json
{
  "status": "ok",
  "message": "logged out"
}
```

### Show Users

List all users.

**Request:**
```json
{"op": "show_users"}
```

**Response:**
```json
{
  "status": "ok",
  "users": [
    {"username": "root", "status": "active", "roles": ["admin"]},
    {"username": "analyst", "status": "active", "roles": ["readonly"]}
  ]
}
```

### Show Roles

List all roles.

**Request:**
```json
{"op": "show_roles"}
```

**Response:**
```json
{
  "status": "ok",
  "roles": [
    {"name": "admin", "builtin": true},
    {"name": "readonly", "builtin": true},
    {"name": "analyst", "builtin": false}
  ]
}
```

### Create User

**Request:**
```json
{
  "op": "create_user",
  "username": "newuser",
  "password": "SecurePass123"
}
```

### Drop User

**Request:**
```json
{
  "op": "drop_user",
  "username": "olduser"
}
```

### Lock/Unlock User

**Request:**
```json
{"op": "lock_user", "username": "suspicious"}
{"op": "unlock_user", "username": "restored"}
```

### Alter User Password

**Request:**
```json
{
  "op": "alter_user_password",
  "username": "admin",
  "new_password": "NewSecurePass456"
}
```

### Create/Drop Role

**Request:**
```json
{"op": "create_role", "name": "analysts"}
{"op": "drop_role", "name": "analysts"}
```

### Grant/Revoke Role

**Request:**
```json
{"op": "grant_role", "username": "analyst", "role": "readonly"}
{"op": "revoke_role", "username": "analyst", "role": "readonly"}
```

### Grant/Revoke Privilege

**Request:**
```json
{
  "op": "grant_privilege",
  "username": "analyst",
  "privilege": "SELECT",
  "target_type": "database",
  "target_name": "analytics"
}
```

### Show Grants

**Request:**
```json
{
  "op": "show_grants",
  "username": "analyst"
}
```

---

## Cluster Operations

### Cluster Status

Get cluster membership and leader info.

**Request:**
```json
{"op": "cluster_status"}
```

**Response:**
```json
{
  "status": "ok",
  "cluster_status": {
    "node_id": "node1",
    "cluster_id": "prod-cluster",
    "role": "Leader",
    "term": 5,
    "leader_id": "node1",
    "alive_nodes": 3,
    "total_nodes": 3,
    "has_quorum": true
  }
}
```

---

## Error Responses

All errors follow this format:

```json
{
  "status": "error",
  "message": "descriptive error message"
}
```

### Error Categories

| Category | Description |
|----------|-------------|
| `bad_request` | Invalid request format or parameters |
| `auth` | Authentication or authorization failure |
| `not_found` | Resource not found |
| `timeout` | Operation timed out |
| `internal` | Internal server error |
| `io` | I/O error |

---

## Compression

### Request Compression

For `ingest_ipc`, set `compression: "zstd"` to indicate the payload is zstd-compressed.

### Response Compression

Include `accept_compression: "zstd"` in query requests. If compression is applied, the response includes `compression: "zstd"`.

---

## Rate Limits

The server enforces rate limits on authentication:

- **Max attempts**: 10 per IP per 60 seconds
- **Lockout**: Automatic after threshold exceeded

---

## Size Limits

Default limits (configurable via CLI flags):

| Limit | Default | Flag |
|-------|---------|------|
| Max IPC payload | 32 MB | `--max-ipc-bytes` |
| Max frame size | 64 MB | `--max-frame-bytes` |
| Max query length | 32 KB | `--max-query-len` |
| Max connections | 64 | `--max-conns` |

---

## Storage Tuning (Core Config)

These knobs are set via `EngineConfig` when embedding `boyodb-core` and partially exposed as server flags.

| Setting | Purpose | Default |
|---------|---------|---------|
| `index_granularity_rows` | Row count per IPC batch when writing sorted hot data (pruning vs. overhead tradeoff) | `0` (disabled) |
| `tier_warm_after_millis` | Age threshold to mark segments Warm | `3600000` |
| `tier_cold_after_millis` | Age threshold to mark segments Cold | `86400000` |
| `tier_warm_compression` | Recompress Warm segments (e.g. `zstd`) | `null` |
| `tier_cold_compression` | Recompress Cold segments (e.g. `zstd`) | `null` |
| `cache_hot_segments` | Cache Hot segments in memory | `true` |
| `cache_warm_segments` | Cache Warm segments in memory | `true` |
| `cache_cold_segments` | Cache Cold segments in memory | `false` |

boyodb-server flags for tier/caching:

- `--index-granularity-rows <n>`
- `--tier-warm-compression <algo>`
- `--tier-cold-compression <algo>`
- `--cache-hot-segments` / `--no-cache-hot-segments`
- `--cache-warm-segments` / `--no-cache-warm-segments`
- `--cache-cold-segments` / `--no-cache-cold-segments`

boyodb-server plan/prepared cache flags:

- `--plan-cache-size <n>`
- `--plan-cache-ttl-secs <n>`
- `--prepared-cache-size <n>`
- `--prepared-cache-ttl-secs <n>`
