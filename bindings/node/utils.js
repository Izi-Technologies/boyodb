'use strict';
const arrow = require('apache-arrow');

// decodeIPC returns an array of objects for convenience; for large result sets prefer streaming.
function decodeIPC(ipcBuffer) {
  const table = arrow.tableFromIPC(ipcBuffer);
  const out = [];
  for (const row of table) {
    out.push(Object.fromEntries(Object.entries(row)));
  }
  return out;
}

// decodeCountSumAvg expects columns: count (uint64), sum (int64), optional avg (float64)
function decodeCountSumAvg(ipcBuffer) {
  const table = arrow.tableFromIPC(ipcBuffer);
  if (table.numRows === 0) throw new Error('empty aggregate result');
  const row = table.get(0);
  return {
    count: Number(row.count),
    sum: Number(row.sum ?? 0),
    avg: row.avg !== undefined ? Number(row.avg) : undefined,
  };
}

module.exports = { decodeIPC, decodeCountSumAvg };

