'use strict';
const fs = require('fs');
const arrow = require('apache-arrow');

// Generates a small Arrow IPC file with tenant_id, route_id, event_time, watermark_micros, duration_ms.
function main(outFile) {
  const data = {
    watermark_micros: [1n, 5n, 10n],
    event_time: [100n, 200n, 300n],
    tenant_id: [7n, 7n, 8n],
    route_id: [101n, 101n, 102n],
    duration_ms: [10, 20, 30],
  };

  const fields = [
    new arrow.Field('watermark_micros', new arrow.Utf8(), false),
  ];

  // Use BigInt64 for timestamp-like columns to preserve full range.
  const table = arrow.tableFromArrays({
    watermark_micros: arrow.BigInt64Array.from(data.watermark_micros),
    event_time: arrow.BigInt64Array.from(data.event_time),
    tenant_id: arrow.BigInt64Array.from(data.tenant_id),
    route_id: arrow.BigInt64Array.from(data.route_id),
    duration_ms: arrow.Int32Array.from(data.duration_ms),
  });

  const buf = arrow.tableToIPC(table);
  fs.writeFileSync(outFile, Buffer.from(buf));
  console.log(`wrote sample IPC to ${outFile}`);
}

if (require.main === module) {
  const out = process.argv[2] || './sample.ipc';
  main(out);
}

