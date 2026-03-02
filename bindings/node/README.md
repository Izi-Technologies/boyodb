# Node binding

`napi-rs` wrapper over the Rust core. The build expects the workspace root `Cargo.toml` to be used (`npm run build` will drive `cargo`).

Minimal use:

```js
const { Database } = require('./index.node');
const db = new Database({ dataDir: '/tmp/boyodb', shardCount: 4, walMaxBytes: 32_000_000, walMaxSegments: 3, allowManifestImport: true });
// Ingest expects Arrow IPC bytes.
db.ingest(Buffer.from('arrow-ipc'), Date.now() * 1000);           // default round-robin routing
db.ingest(Buffer.from('arrow-ipc'), Date.now() * 1000, 42);        // shard key routing (e.g., tenant hash)
db.ingestInto(Buffer.from('arrow-ipc'), Date.now() * 1000, 42, 'cdrs', 'calls'); // shard + db/table metadata
db.createDatabase('cdrs');
db.createTable('cdrs', 'calls', '{"columns":[{"name":"event_time","type":"timestamp"}]}');
db.healthcheck();
console.log(JSON.parse(db.listDatabases().toString()));
console.log(JSON.parse(db.listTables('cdrs').toString()));
const result = db.query('SELECT 1', 1000);
console.log(result.toString());
// Note: Query returns Arrow IPC bytes; decode with Apache Arrow JS.

// Export manifest JSON
console.log(JSON.parse(db.manifest().toString()));
// Import manifest (trusted path only)
// db.importManifest(Buffer.from(JSON.stringify(manifest)), true);

// Plan a replication bundle capped to ~10MB and throttled to ~5MB/s
const plan = db.planBundle(true, 10_000_000, 0, 5_000_000);
console.log(JSON.parse(plan.toString()));
```

CLI:
```
npm run build
node cli.js ingest /tmp/boyodb ./sample.ipc cdrs.calls
node cli.js query /tmp/boyodb "SELECT COUNT(*) FROM cdrs.calls WHERE tenant_id=7"
node cli.js manifest /tmp/boyodb
node cli.js import-manifest /tmp/boyodb ./manifest.json --overwrite
node cli.js health /tmp/boyodb
node make-sample-ipc.js ./sample.ipc   # generate a tiny Arrow IPC file
```

Helpers: `utils.js` provides `decodeIPC` and `decodeCountSumAvg` for Arrow IPC query results.
