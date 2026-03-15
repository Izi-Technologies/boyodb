/**
 * Async Insert support for BoyoDB Node.js driver.
 *
 * Provides high-throughput batch inserts with automatic buffering and deduplication.
 */

/**
 * Configuration for async insert buffer.
 * @typedef {Object} AsyncInsertConfig
 * @property {number} [maxRows=10000] - Maximum rows before flush
 * @property {number} [maxBytes=10485760] - Maximum bytes before flush (10MB)
 * @property {number} [maxWaitMs=200] - Maximum wait time before flush in milliseconds
 * @property {boolean} [deduplicate=false] - Enable deduplication
 * @property {string[]} [dedupColumns=[]] - Columns to use for deduplication
 */

/**
 * Statistics for async insert operations.
 * @typedef {Object} AsyncInsertStats
 * @property {number} totalRows - Total rows inserted
 * @property {number} totalFlushes - Number of flush operations
 * @property {number} totalBytes - Total bytes inserted
 * @property {number} duplicatesRemoved - Duplicates removed (if dedup enabled)
 * @property {number} errors - Number of errors
 */

/**
 * Async insert buffer for high-throughput batch inserts.
 *
 * @example
 * const buffer = new AsyncInsertBuffer(client, 'mydb.events', {
 *   maxRows: 10000,
 *   maxWaitMs: 200,
 *   deduplicate: true,
 *   dedupColumns: ['id']
 * });
 *
 * buffer.start();
 *
 * for (let i = 0; i < 100000; i++) {
 *   buffer.insert({ id: i, event: 'click', value: i * 10 });
 * }
 *
 * await buffer.stop();
 * console.log(buffer.stats);
 */
class AsyncInsertBuffer {
  /**
   * Create a new async insert buffer.
   * @param {Client} client - BoyoDB client
   * @param {string} table - Target table (database.table)
   * @param {AsyncInsertConfig} [config={}] - Buffer configuration
   */
  constructor(client, table, config = {}) {
    this._client = client;
    this._table = table;
    this._maxRows = config.maxRows || 10000;
    this._maxBytes = config.maxBytes || 10 * 1024 * 1024;
    this._maxWaitMs = config.maxWaitMs || 200;
    this._deduplicate = config.deduplicate || false;
    this._dedupColumns = config.dedupColumns || [];

    this._buffer = [];
    this._bufferBytes = 0;
    this._running = false;
    this._flushTimer = null;
    this._flushPromise = null;

    this._stats = {
      totalRows: 0,
      totalFlushes: 0,
      totalBytes: 0,
      duplicatesRemoved: 0,
      errors: 0,
    };

    // Deduplication set
    this._seen = new Set();
  }

  /**
   * Start the async insert buffer.
   */
  start() {
    if (this._running) return;
    this._running = true;
    this._scheduleFlush();
  }

  /**
   * Stop the buffer and flush remaining rows.
   * @returns {Promise<void>}
   */
  async stop() {
    this._running = false;
    if (this._flushTimer) {
      clearTimeout(this._flushTimer);
      this._flushTimer = null;
    }
    await this.flush();
  }

  /**
   * Insert a row into the buffer.
   * @param {Object} row - Row data as key-value object
   */
  insert(row) {
    if (!this._running) {
      throw new Error('Buffer not started');
    }

    // Handle deduplication
    if (this._deduplicate && this._dedupColumns.length > 0) {
      const key = this._dedupColumns.map(col => row[col]).join('|');
      if (this._seen.has(key)) {
        this._stats.duplicatesRemoved++;
        return;
      }
      this._seen.add(key);
    }

    const rowStr = JSON.stringify(row);
    const rowBytes = Buffer.byteLength(rowStr, 'utf8');

    this._buffer.push(row);
    this._bufferBytes += rowBytes;

    // Check if we need to flush
    if (this._buffer.length >= this._maxRows || this._bufferBytes >= this._maxBytes) {
      this._triggerFlush();
    }
  }

  /**
   * Insert multiple rows.
   * @param {Object[]} rows - Array of row objects
   */
  insertMany(rows) {
    for (const row of rows) {
      this.insert(row);
    }
  }

  /**
   * Force flush the buffer.
   * @returns {Promise<void>}
   */
  async flush() {
    if (this._buffer.length === 0) return;

    const rows = this._buffer;
    const bytes = this._bufferBytes;

    this._buffer = [];
    this._bufferBytes = 0;
    if (this._deduplicate) {
      this._seen.clear();
    }

    try {
      // Build batch insert SQL
      const columns = Object.keys(rows[0]);
      const values = rows.map(row => {
        const vals = columns.map(col => {
          const v = row[col];
          if (v === null || v === undefined) return 'NULL';
          if (typeof v === 'string') return `'${v.replace(/'/g, "''")}'`;
          if (typeof v === 'boolean') return v ? 'TRUE' : 'FALSE';
          if (v instanceof Date) return `'${v.toISOString()}'`;
          if (Array.isArray(v)) return `'${JSON.stringify(v)}'`;
          if (typeof v === 'object') return `'${JSON.stringify(v)}'`;
          return String(v);
        });
        return `(${vals.join(', ')})`;
      });

      const sql = `INSERT INTO ${this._table} (${columns.join(', ')}) VALUES ${values.join(', ')}`;
      await this._client.exec(sql);

      this._stats.totalRows += rows.length;
      this._stats.totalBytes += bytes;
      this._stats.totalFlushes++;
    } catch (err) {
      this._stats.errors++;
      throw err;
    }
  }

  /**
   * Get buffer statistics.
   * @returns {AsyncInsertStats}
   */
  get stats() {
    return { ...this._stats };
  }

  /**
   * Get current buffer size.
   * @returns {number}
   */
  get bufferSize() {
    return this._buffer.length;
  }

  /**
   * Check if buffer is running.
   * @returns {boolean}
   */
  get isRunning() {
    return this._running;
  }

  _scheduleFlush() {
    if (!this._running) return;

    this._flushTimer = setTimeout(() => {
      this._triggerFlush();
      this._scheduleFlush();
    }, this._maxWaitMs);
  }

  _triggerFlush() {
    if (this._flushPromise) return;

    this._flushPromise = this.flush()
      .catch(err => {
        console.error('Async insert flush error:', err);
      })
      .finally(() => {
        this._flushPromise = null;
      });
  }
}

module.exports = {
  AsyncInsertBuffer,
};
