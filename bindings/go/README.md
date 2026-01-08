# Go binding

cgo wrapper targeting the Rust C ABI in `boyodb-core`.

```
go test ./...
```

Link flags expect `libboyodb_core` to be available in `target/release` or `target/debug`; adjust `#cgo LDFLAGS` in `boyodb.go` as needed.

CLI helper (parity with Node/Rust CLIs) lives at `bindings/go/cmd/boyodb-go-cli`:
```
go run ./cmd/boyodb-go-cli ingest /tmp/boyodb batch.ipc mydb.events
go run ./cmd/boyodb-go-cli query /tmp/boyodb "SELECT COUNT(*) FROM mydb.events"
go run ./cmd/boyodb-go-cli create-db /tmp/boyodb mydb
go run ./cmd/boyodb-go-cli create-table /tmp/boyodb mydb.events schema.json
go run ./cmd/boyodb-go-cli list-dbs /tmp/boyodb
go run ./cmd/boyodb-go-cli list-tables /tmp/boyodb mydb
go run ./cmd/boyodb-go-cli manifest /tmp/boyodb
go run ./cmd/boyodb-go-cli import-manifest /tmp/boyodb manifest.json --overwrite
go run ./cmd/boyodb-go-cli health /tmp/boyodb
```

Example sketch:
```go
handle, _ := boyodb.Open(boyodb.OpenOptions{
    DataDir: "/tmp/boyodb",
    ShardCount: 4,
    WalMaxBytes: 32_000_000,
    WalMaxSegments: 3,
    AllowManifestImport: true, // only if using a trusted replication path
})
defer handle.Close()
// Ingest expects Arrow IPC bytes.
_ = handle.Ingest([]byte("arrow-ipc"), uint64(time.Now().UnixMicro()))
_ = handle.IngestWithShard([]byte("arrow-ipc"), uint64(time.Now().UnixMicro()), 42)
_ = handle.IngestInto([]byte("arrow-ipc"), uint64(time.Now().UnixMicro()), 42, true, "analytics", "events")
_ = handle.CreateDatabase("analytics")
_ = handle.CreateTable("analytics", "events", `{"columns":[{"name":"event_time","type":"timestamp"}]}`)
_ = handle.HealthCheck()
dbs, _ := handle.ListDatabases()
tables, _ := handle.ListTables("analytics")
rows, _ := handle.Query("SELECT * FROM analytics.events", 1000)
plan, _ := handle.PlanBundle(boyodb.BundleRequest{
    MaxBytes: 10_000_000, HasMaxBytes: true,
    TargetBytesPerSec: 5_000_000, HasTargetBytesPerSec: true,
    PreferHot: true,
})
fmt.Println(string(plan))
fmt.Println(dbs, tables, string(rows))
// Note: Query returns Arrow IPC bytes; decode with Apache Arrow Go reader.

// Export manifest for debugging/replication
manifestJSON, _ := handle.Manifest()
fmt.Println(string(manifestJSON))
agg, _ := boyodb.DecodeCountSumAvg(rows) // or DecodeRows for non-aggregate queries
```
