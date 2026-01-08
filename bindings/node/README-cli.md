# Node CLI

Minimal helper using the Node binding. Requires `npm install` (for `apache-arrow`).

Examples:
```
npm run build
node cli.js ingest /tmp/boyodb ./sample.ipc analytics.events
node cli.js query /tmp/boyodb "SELECT COUNT(*) FROM analytics.events WHERE tenant_id=7"
node cli.js manifest /tmp/boyodb
node cli.js import-manifest /tmp/boyodb ./manifest.json --overwrite
node cli.js health /tmp/boyodb
```

