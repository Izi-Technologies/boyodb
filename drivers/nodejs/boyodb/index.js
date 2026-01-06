/**
 * boyodb Node.js Driver
 *
 * A Node.js client for connecting to boyodb-server.
 *
 * @example
 * const { Client } = require('boyodb');
 *
 * const client = new Client('localhost:8765');
 * await client.connect();
 *
 * const result = await client.query('SELECT * FROM users LIMIT 10');
 * for (const row of result.rows) {
 *   console.log(row);
 * }
 *
 * await client.close();
 */

const net = require('net');
const tls = require('tls');
const fs = require('fs');
const { tableFromIPC } = require('apache-arrow');

/**
 * Configuration options for the boyodb client.
 * @typedef {Object} ClientConfig
 * @property {boolean} [tls=false] - Enable TLS encryption
 * @property {string} [caFile] - Path to CA certificate file
 * @property {boolean} [insecureSkipVerify=false] - Skip TLS verification.
 *   WARNING: SECURITY RISK - Disables certificate validation, making connections
 *   vulnerable to man-in-the-middle (MITM) attacks. NEVER use in production.
 * @property {number} [connectTimeout=10000] - Connection timeout in milliseconds
 * @property {number} [readTimeout=30000] - Read timeout in milliseconds
 * @property {number} [writeTimeout=10000] - Write timeout in milliseconds
 * @property {string} [token] - Authentication token
 * @property {number} [maxRetries=3] - Maximum connection retry attempts
 * @property {number} [retryDelay=1000] - Delay between retries in milliseconds
 * @property {string} [database] - Default database for queries
 * @property {number} [queryTimeout=30000] - Default query timeout in milliseconds
 */

/**
 * Query result object.
 * @typedef {Object} QueryResult
 * @property {Array<Object>} rows - Array of row objects
 * @property {Array<string>} columns - Column names
 * @property {number} rowCount - Total number of rows
 * @property {number} segmentsScanned - Number of segments scanned
 * @property {number} dataSkippedBytes - Bytes skipped due to pruning
 */

/**
 * boyodb client for Node.js
 */
class Client {
  /**
   * Create a new boyodb client.
   * @param {string} host - Server address in host:port format
   * @param {ClientConfig} [config={}] - Configuration options
   */
  constructor(host, config = {}) {
    const [hostname, port] = host.split(':');
    this.hostname = hostname || 'localhost';
    this.port = parseInt(port) || 8765;

    this.config = {
      tls: config.tls || false,
      caFile: config.caFile || null,
      insecureSkipVerify: config.insecureSkipVerify || false,
      connectTimeout: config.connectTimeout || 10000,
      readTimeout: config.readTimeout || 30000,
      writeTimeout: config.writeTimeout || 10000,
      token: config.token || null,
      maxRetries: config.maxRetries || 3,
      retryDelay: config.retryDelay || 1000,
      database: config.database || null,
      queryTimeout: config.queryTimeout || 30000,
    };

    this.socket = null;
    this.sessionId = null;
    this._pendingData = Buffer.alloc(0);
  }

  /**
   * Connect to the server.
   * @returns {Promise<void>}
   */
  async connect() {
    await this._connect();

    // Verify connection with health check
    try {
      await this.health();
    } catch (err) {
      await this.close();
      throw new Error(`Health check failed: ${err.message}`);
    }
  }

  /**
   * Internal connection method with retry logic.
   * @private
   */
  async _connect() {
    let lastError = null;

    for (let attempt = 0; attempt < this.config.maxRetries; attempt++) {
      try {
        await this._connectOnce();
        return;
      } catch (err) {
        lastError = err;
        if (attempt < this.config.maxRetries - 1) {
          await this._sleep(this.config.retryDelay);
        }
      }
    }

    throw new Error(`Failed to connect after ${this.config.maxRetries} attempts: ${lastError.message}`);
  }

  /**
   * Single connection attempt.
   * @private
   */
  async _connectOnce() {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Connection timeout'));
      }, this.config.connectTimeout);

      const options = {
        host: this.hostname,
        port: this.port,
      };

      let socket;
      if (this.config.tls) {
        if (this.config.insecureSkipVerify) {
          console.warn('WARNING: TLS certificate verification is DISABLED. ' +
            'This is insecure and vulnerable to MITM attacks. ' +
            'Only use for testing with self-signed certificates.');
        }

        const tlsOptions = {
          ...options,
          rejectUnauthorized: !this.config.insecureSkipVerify,
        };

        if (this.config.caFile) {
          tlsOptions.ca = fs.readFileSync(this.config.caFile);
        }

        socket = tls.connect(tlsOptions, () => {
          clearTimeout(timeout);
          this.socket = socket;
          resolve();
        });
      } else {
        socket = net.connect(options, () => {
          clearTimeout(timeout);
          this.socket = socket;
          resolve();
        });
      }

      socket.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }

  /**
   * Close the connection.
   * @returns {Promise<void>}
   */
  async close() {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
  }

  /**
   * Send a request to the server.
   * @private
   * @param {Object} request - Request object
   * @returns {Promise<Object>} - Response object
   */
  async _sendRequest(request) {
    if (!this.socket) {
      throw new Error('Not connected');
    }

    // Add auth if available
    if (this.sessionId) {
      request.auth = this.sessionId;
    } else if (this.config.token) {
      request.auth = this.config.token;
    }

    const json = JSON.stringify(request);
    const payload = Buffer.from(json, 'utf8');

    // Create length-prefixed frame
    const frame = Buffer.alloc(4 + payload.length);
    frame.writeUInt32BE(payload.length, 0);
    payload.copy(frame, 4);

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Request timeout'));
      }, this.config.readTimeout);

      let responseBuffer = Buffer.alloc(0);
      let expectedLength = null;
      let headerResponse = null;
      let stage = 'header';
      const ipcChunks = [];

      const onData = (data) => {
        responseBuffer = Buffer.concat([responseBuffer, data]);

        while (true) {
          if (expectedLength === null) {
            if (responseBuffer.length < 4) {
              return;
            }
            expectedLength = responseBuffer.readUInt32BE(0);
            responseBuffer = responseBuffer.slice(4);
          }

          if (responseBuffer.length < expectedLength) {
            return;
          }

          const frame = responseBuffer.slice(0, expectedLength);
          responseBuffer = responseBuffer.slice(expectedLength);
          expectedLength = null;

          if (stage === 'header') {
            try {
              const response = JSON.parse(frame.toString('utf8'));
              if (response.ipc_streaming) {
                headerResponse = response;
                stage = 'ipc_stream';
                continue;
              }
              if (response.ipc_len) {
                headerResponse = response;
                stage = 'ipc';
                continue;
              }
              clearTimeout(timeout);
              this.socket.removeListener('data', onData);
              this.socket.removeListener('error', onError);
              resolve(response);
              return;
            } catch (err) {
              clearTimeout(timeout);
              this.socket.removeListener('data', onData);
              this.socket.removeListener('error', onError);
              reject(new Error(`Failed to parse response: ${err.message}`));
              return;
            }
          } else if (stage === 'ipc') {
            if (!headerResponse) {
              clearTimeout(timeout);
              this.socket.removeListener('data', onData);
              this.socket.removeListener('error', onError);
              reject(new Error('Missing response header for IPC payload'));
              return;
            }
            if (frame.length !== headerResponse.ipc_len) {
              clearTimeout(timeout);
              this.socket.removeListener('data', onData);
              this.socket.removeListener('error', onError);
              reject(new Error(`IPC length mismatch: expected ${headerResponse.ipc_len} got ${frame.length}`));
              return;
            }
            headerResponse.ipc_bytes = frame;
            clearTimeout(timeout);
            this.socket.removeListener('data', onData);
            this.socket.removeListener('error', onError);
            resolve(headerResponse);
            return;
          } else if (stage === 'ipc_stream') {
            if (!headerResponse) {
              clearTimeout(timeout);
              this.socket.removeListener('data', onData);
              this.socket.removeListener('error', onError);
              reject(new Error('Missing response header for IPC payload'));
              return;
            }
            if (frame.length === 0) {
              headerResponse.ipc_bytes = Buffer.concat(ipcChunks);
              clearTimeout(timeout);
              this.socket.removeListener('data', onData);
              this.socket.removeListener('error', onError);
              resolve(headerResponse);
              return;
            }
            ipcChunks.push(frame);
          }
        }
      };

      const onError = (err) => {
        clearTimeout(timeout);
        this.socket.removeListener('data', onData);
        reject(err);
      };

      this.socket.on('data', onData);
      this.socket.on('error', onError);

      this.socket.write(frame);
    });
  }

  /**
   * Check server health.
   * @returns {Promise<void>}
   */
  async health() {
    const response = await this._sendRequest({ op: 'health' });
    if (response.status !== 'ok') {
      throw new Error(response.message || 'Health check failed');
    }
  }

  /**
   * Login with username and password.
   * @param {string} username - Username
   * @param {string} password - Password
   * @returns {Promise<void>}
   */
  async login(username, password) {
    const response = await this._sendRequest({
      op: 'login',
      username,
      password,
    });

    if (response.status !== 'ok') {
      throw new Error(response.message || 'Login failed');
    }

    this.sessionId = response.session_id;
  }

  /**
   * Logout from the server.
   * @returns {Promise<void>}
   */
  async logout() {
    const response = await this._sendRequest({ op: 'logout' });
    if (response.status !== 'ok') {
      throw new Error(response.message || 'Logout failed');
    }
    this.sessionId = null;
  }

  /**
   * Execute a SQL query.
   * @param {string} sql - SQL query string
   * @param {Object} [options={}] - Query options
   * @param {string} [options.database] - Database to use
   * @param {number} [options.timeout] - Query timeout in milliseconds
   * @returns {Promise<QueryResult>}
   */
  async query(sql, options = {}) {
    const database = options.database || this.config.database;
    const timeout = options.timeout || this.config.queryTimeout;

    const isBinary = this._isSelectLike(sql);
    const request = {
      op: isBinary ? 'query_binary' : 'query',
      sql,
      timeout_millis: timeout,
    };
    if (isBinary) {
      request.stream = true;
    }

    if (database) {
      request.database = database;
    }

    const response = await this._sendRequest(request);

    if (response.status !== 'ok') {
      throw new Error(response.message || 'Query failed');
    }

    // Parse Arrow IPC data if present
    let rows = [];
    let columns = [];

    if (response.ipc_bytes) {
      const parsed = this._parseArrowIPC(response.ipc_bytes);
      rows = parsed.rows;
      columns = parsed.columns;
    } else if (response.ipc_base64) {
      const ipcData = Buffer.from(response.ipc_base64, 'base64');
      const parsed = this._parseArrowIPC(ipcData);
      rows = parsed.rows;
      columns = parsed.columns;
    }

    return {
      rows,
      columns,
      rowCount: rows.length,
      segmentsScanned: response.segments_scanned || 0,
      dataSkippedBytes: response.data_skipped_bytes || 0,
    };
  }

  /**
   * Prepare a SELECT query on the server and return a prepared id.
   * @param {string} sql - SQL query string
   * @param {Object} [options={}] - Prepare options
   * @param {string} [options.database] - Database to use
   * @returns {Promise<string>}
   */
  async prepare(sql, options = {}) {
    const database = options.database || this.config.database;
    const request = { op: 'prepare', sql };
    if (database) {
      request.database = database;
    }
    const response = await this._sendRequest(request);
    if (response.status !== 'ok') {
      throw new Error(response.message || 'Prepare failed');
    }
    if (!response.prepared_id) {
      throw new Error('missing prepared_id in response');
    }
    return response.prepared_id;
  }

  /**
   * Execute a prepared statement using binary IPC responses.
   * @param {string} preparedId - Prepared statement id
   * @param {Object} [options={}] - Execution options
   * @param {number} [options.timeout] - Query timeout in milliseconds
   * @returns {Promise<QueryResult>}
   */
  async executePreparedBinary(preparedId, options = {}) {
    const timeout = options.timeout || this.config.queryTimeout;
    const response = await this._sendRequest({
      op: 'execute_prepared_binary',
      id: preparedId,
      timeout_millis: timeout,
      stream: true,
    });
    if (response.status !== 'ok') {
      throw new Error(response.message || 'Execute prepared failed');
    }

    let rows = [];
    let columns = [];

    if (response.ipc_bytes) {
      const parsed = this._parseArrowIPC(response.ipc_bytes);
      rows = parsed.rows;
      columns = parsed.columns;
    } else if (response.ipc_base64) {
      const ipcData = Buffer.from(response.ipc_base64, 'base64');
      const parsed = this._parseArrowIPC(ipcData);
      rows = parsed.rows;
      columns = parsed.columns;
    }

    return {
      rows,
      columns,
      rowCount: rows.length,
      segmentsScanned: response.segments_scanned || 0,
      dataSkippedBytes: response.data_skipped_bytes || 0,
    };
  }

  /**
   * Execute a SQL statement that doesn't return rows.
   * @param {string} sql - SQL statement
   * @param {Object} [options={}] - Execution options
   * @returns {Promise<void>}
   */
  async exec(sql, options = {}) {
    const result = await this.query(sql, options);
    return result;
  }

  _isSelectLike(sql) {
    const trimmed = sql.trimStart().toLowerCase();
    return trimmed.startsWith('select ') || trimmed.startsWith('with ');
  }

  /**
   * Create a new database.
   * @param {string} name - Database name
   * @returns {Promise<void>}
   */
  async createDatabase(name) {
    const response = await this._sendRequest({
      op: 'createdatabase',
      name,
    });

    if (response.status !== 'ok') {
      throw new Error(response.message || 'Create database failed');
    }
  }

  /**
   * Create a new table.
   * @param {string} database - Database name
   * @param {string} table - Table name
   * @returns {Promise<void>}
   */
  async createTable(database, table) {
    const response = await this._sendRequest({
      op: 'createtable',
      database,
      table,
    });

    if (response.status !== 'ok') {
      throw new Error(response.message || 'Create table failed');
    }
  }

  /**
   * List all databases.
   * @returns {Promise<Array<string>>}
   */
  async listDatabases() {
    const response = await this._sendRequest({ op: 'listdatabases' });

    if (response.status !== 'ok') {
      throw new Error(response.message || 'List databases failed');
    }

    return response.databases || [];
  }

  /**
   * List tables, optionally filtered by database.
   * @param {string} [database] - Database to filter by
   * @returns {Promise<Array<{database: string, name: string}>>}
   */
  async listTables(database = null) {
    const request = { op: 'listtables' };
    if (database) {
      request.database = database;
    }

    const response = await this._sendRequest(request);

    if (response.status !== 'ok') {
      throw new Error(response.message || 'List tables failed');
    }

    return response.tables || [];
  }

  /**
   * Get query execution plan.
   * @param {string} sql - SQL query
   * @returns {Promise<Object>}
   */
  async explain(sql) {
    const response = await this._sendRequest({
      op: 'explain',
      sql,
    });

    if (response.status !== 'ok') {
      throw new Error(response.message || 'Explain failed');
    }

    return response.explain_plan;
  }

  /**
   * Get server metrics.
   * @returns {Promise<Object>}
   */
  async metrics() {
    const response = await this._sendRequest({ op: 'metrics' });

    if (response.status !== 'ok') {
      throw new Error(response.message || 'Metrics failed');
    }

    return response.metrics;
  }

  /**
   * Ingest CSV data into a table.
   * @param {string} database - Database name
   * @param {string} table - Table name
   * @param {Buffer|string} csvData - CSV data
   * @param {Object} [options={}] - Ingest options
   * @param {boolean} [options.hasHeader=true] - CSV has header row
   * @param {string} [options.delimiter=','] - Field delimiter
   * @returns {Promise<void>}
   */
  async ingestCSV(database, table, csvData, options = {}) {
    const buffer = Buffer.isBuffer(csvData) ? csvData : Buffer.from(csvData, 'utf8');

    const request = {
      op: 'ingestcsv',
      database,
      table,
      payload_base64: buffer.toString('base64'),
      has_header: options.hasHeader !== false,
    };

    if (options.delimiter) {
      request.delimiter = options.delimiter;
    }

    const response = await this._sendRequest(request);

    if (response.status !== 'ok') {
      throw new Error(response.message || 'Ingest CSV failed');
    }
  }

  /**
   * Ingest Arrow IPC data into a table.
   * @param {string} database - Database name
   * @param {string} table - Table name
   * @param {Buffer} ipcData - Arrow IPC data
   * @returns {Promise<void>}
   */
  async ingestIPC(database, table, ipcData) {
    try {
      await this._sendRequestBinary({
        op: 'ingest_ipc_binary',
        database,
        table,
      }, ipcData);
      return;
    } catch (err) {
      // Fall back to base64 on older servers.
    }
    const response = await this._sendRequest({
      op: 'ingestipc',
      database,
      table,
      payload_base64: ipcData.toString('base64'),
    });

    if (response.status !== 'ok') {
      throw new Error(response.message || 'Ingest IPC failed');
    }
  }

  _sendRequestBinary(request, payload) {
    if (!this.socket) {
      return Promise.reject(new Error('Not connected'));
    }

    if (this.sessionId) {
      request.auth = this.sessionId;
    } else if (this.config.token) {
      request.auth = this.config.token;
    }

    const json = JSON.stringify(request);
    const header = Buffer.from(json, 'utf8');
    const headerFrame = Buffer.alloc(4 + header.length);
    headerFrame.writeUInt32BE(header.length, 0);
    header.copy(headerFrame, 4);

    const payloadFrame = Buffer.alloc(4 + payload.length);
    payloadFrame.writeUInt32BE(payload.length, 0);
    payload.copy(payloadFrame, 4);

    const frame = Buffer.concat([headerFrame, payloadFrame]);

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Request timeout'));
      }, this.config.readTimeout);

      let responseBuffer = Buffer.alloc(0);
      let expectedLength = null;

      const onData = (data) => {
        responseBuffer = Buffer.concat([responseBuffer, data]);

        if (expectedLength === null && responseBuffer.length >= 4) {
          expectedLength = responseBuffer.readUInt32BE(0);
          responseBuffer = responseBuffer.slice(4);
        }

        if (expectedLength !== null && responseBuffer.length >= expectedLength) {
          clearTimeout(timeout);
          this.socket.removeListener('data', onData);
          this.socket.removeListener('error', onError);

          try {
            const responseJson = responseBuffer.slice(0, expectedLength).toString('utf8');
            const response = JSON.parse(responseJson);
            if (response.status !== 'ok') {
              reject(new Error(response.message || 'Ingest IPC failed'));
              return;
            }
            resolve();
          } catch (err) {
            reject(new Error(`Failed to parse response: ${err.message}`));
          }
        }
      };

      const onError = (err) => {
        clearTimeout(timeout);
        this.socket.removeListener('data', onData);
        reject(err);
      };

      this.socket.on('data', onData);
      this.socket.on('error', onError);

      this.socket.write(frame);
    });
  }

  /**
   * Set the default database.
   * @param {string} database - Database name
   */
  setDatabase(database) {
    this.config.database = database;
  }

  /**
   * Set the authentication token.
   * @param {string} token - Auth token
   */
  setToken(token) {
    this.config.token = token;
  }

  /**
  * Parse Arrow IPC stream format using apache-arrow.
   *
   * @private
   * @param {Buffer} data - IPC data
   * @returns {{rows: Array, columns: Array<string>}}
   */
  _parseArrowIPC(data) {
    try {
      const table = tableFromIPC(data);
      const columns = table.schema.fields.map((f) => f.name);
      const rows = [];

      for (let rowIdx = 0; rowIdx < table.length; rowIdx++) {
        const row = {};
        for (let colIdx = 0; colIdx < columns.length; colIdx++) {
          const vector = table.getColumnAt(colIdx);
          row[columns[colIdx]] = vector ? vector.get(rowIdx) : null;
        }
        rows.push(row);
      }

      return { rows, columns };
    } catch (err) {
      throw new Error(`Failed to parse Arrow IPC: ${err.message}`);
    }
  }

  /**
   * Sleep for a duration.
   * @private
   */
  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = { Client };
