'use strict';
const fs = require('fs');
const { Database } = require('./index.node');
const { decodeIPC } = require('./utils');

function usage() {
  console.log(`Usage:
  node cli.js ingest <dataDir> <ipcFile> [db.table]          # ingest Arrow IPC file
  node cli.js query <dataDir> "<SQL>"                        # run query, print decoded rows
  node cli.js create-db <dataDir> <name>                     # create database
  node cli.js create-table <dataDir> <db>.<table> [schema.json] # create table (optional schema JSON)
  node cli.js list-dbs <dataDir>                             # list databases
  node cli.js list-tables <dataDir> [db]                     # list tables (optionally within db)
  node cli.js manifest <dataDir>                             # export manifest JSON
  node cli.js import-manifest <dataDir> <jsonFile> [--overwrite]
  node cli.js health <dataDir>`);
}

async function main() {
  const args = process.argv.slice(2);
  if (args.length < 2) {
    usage();
    process.exit(1);
  }
  const cmd = args[0];
  const dataDir = args[1];
  const db = new Database({ dataDir });

  try {
    if (cmd === 'ingest') {
      if (args.length < 3) return usage();
      const ipcPath = args[2];
      const dbTable = args[3];
      let database, table;
      if (dbTable && dbTable.includes('.')) {
        [database, table] = dbTable.split('.');
      }
      const payload = fs.readFileSync(ipcPath);
      db.ingestInto(payload, Date.now() * 1000, undefined, database, table);
      console.log('ingest ok');
    } else if (cmd === 'query') {
      const sql = args[2];
      const buf = db.query(sql, 10000);
      const rows = decodeIPC(buf);
      console.log(JSON.stringify(rows, null, 2));
    } else if (cmd === 'create-db') {
      const name = args[2];
      if (!name) return usage();
      db.createDatabase(name);
      console.log(`database created: ${name}`);
    } else if (cmd === 'create-table') {
      const target = args[2];
      if (!target || !target.includes('.')) return usage();
      const [database, table] = target.split('.');
      const schemaFile = args[3];
      const schemaJson = schemaFile ? fs.readFileSync(schemaFile, 'utf8') : undefined;
      db.createTable(database, table, schemaJson);
      console.log(`table created: ${database}.${table}`);
    } else if (cmd === 'list-dbs') {
      const buf = db.listDatabases();
      console.log(buf.toString());
    } else if (cmd === 'list-tables') {
      const database = args[2];
      const buf = db.listTables(database);
      console.log(buf.toString());
    } else if (cmd === 'manifest') {
      const m = db.manifest();
      console.log(m.toString());
    } else if (cmd === 'import-manifest') {
      const jsonFile = args[2];
      const overwrite = args.includes('--overwrite');
      const payload = fs.readFileSync(jsonFile);
      db.importManifest(payload, overwrite);
      console.log('manifest imported');
    } else if (cmd === 'health') {
      db.healthcheck();
      console.log('healthy');
    } else {
      usage();
      process.exit(1);
    }
  } catch (e) {
    console.error('error:', e.message || e);
    process.exit(1);
  }
}

main();
