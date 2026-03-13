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

  // Transaction Support

  /**
   * Start a new transaction.
   * @param {Object} [options={}] - Transaction options
   * @param {string} [options.isolationLevel] - Isolation level (READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE)
   * @param {boolean} [options.readOnly=false] - Start a read-only transaction
   * @returns {Promise<void>}
   */
  async begin(options = {}) {
    let sql = 'BEGIN';
    if (options.isolationLevel) {
      sql = `START TRANSACTION ISOLATION LEVEL ${options.isolationLevel}`;
      if (options.readOnly) {
        sql += ' READ ONLY';
      }
    } else if (options.readOnly) {
      sql = 'START TRANSACTION READ ONLY';
    }
    await this.exec(sql);
  }

  /**
   * Commit the current transaction.
   * @returns {Promise<void>}
   */
  async commit() {
    await this.exec('COMMIT');
  }

  /**
   * Rollback the current transaction or to a savepoint.
   * @param {string} [savepoint] - If provided, rollback to this savepoint
   * @returns {Promise<void>}
   */
  async rollback(savepoint = null) {
    if (savepoint) {
      await this.exec(`ROLLBACK TO SAVEPOINT ${savepoint}`);
    } else {
      await this.exec('ROLLBACK');
    }
  }

  /**
   * Create a savepoint.
   * @param {string} name - Savepoint name
   * @returns {Promise<void>}
   */
  async savepoint(name) {
    await this.exec(`SAVEPOINT ${name}`);
  }

  /**
   * Release a savepoint.
   * @param {string} name - Savepoint name
   * @returns {Promise<void>}
   */
  async releaseSavepoint(name) {
    await this.exec(`RELEASE SAVEPOINT ${name}`);
  }

  // ==========================================
  // Pub/Sub Support
  // ==========================================

  /**
   * Start listening on a notification channel.
   * @param {string} channel - Channel name to listen on
   * @returns {Promise<void>}
   */
  async listen(channel) {
    const safeChannel = this._escapeIdentifier(channel);
    await this.exec(`LISTEN ${safeChannel}`);
  }

  /**
   * Stop listening on a notification channel.
   * @param {string} [channel='*'] - Channel name (use '*' to unlisten all)
   * @returns {Promise<void>}
   */
  async unlisten(channel = '*') {
    if (channel === '*') {
      await this.exec('UNLISTEN *');
    } else {
      const safeChannel = this._escapeIdentifier(channel);
      await this.exec(`UNLISTEN ${safeChannel}`);
    }
  }

  /**
   * Send a notification on a channel.
   * @param {string} channel - Channel name
   * @param {string} [payload] - Optional message payload
   * @returns {Promise<void>}
   */
  async notify(channel, payload = null) {
    const safeChannel = this._escapeIdentifier(channel);
    if (payload !== null) {
      const safePayload = this._escapeString(payload);
      await this.exec(`NOTIFY ${safeChannel}, '${safePayload}'`);
    } else {
      await this.exec(`NOTIFY ${safeChannel}`);
    }
  }

  // ==========================================
  // Trigger Management
  // ==========================================

  /**
   * Create a database trigger.
   * @param {string} name - Trigger name
   * @param {string} table - Target table (format: database.table)
   * @param {Object} options - Trigger options
   * @param {string} options.timing - When to fire: 'BEFORE', 'AFTER', or 'INSTEAD OF'
   * @param {string|Array<string>} options.events - Trigger events: 'INSERT', 'UPDATE', 'DELETE', or array of events
   * @param {string} options.function - Function name to call
   * @param {Array} [options.arguments] - Arguments to pass to the function
   * @param {boolean} [options.forEachRow=true] - Fire for each row (vs statement)
   * @param {string} [options.when] - Optional WHEN condition
   * @param {boolean} [options.orReplace=false] - Replace if exists
   * @returns {Promise<void>}
   */
  async createTrigger(name, table, options) {
    const safeName = this._escapeIdentifier(name);
    const safeTable = this._escapeIdentifier(table);
    const timing = (options.timing || 'AFTER').toUpperCase();

    const events = Array.isArray(options.events)
      ? options.events.map(e => e.toUpperCase()).join(' OR ')
      : options.events.toUpperCase();

    const forEachRow = options.forEachRow !== false;
    const safeFunction = this._escapeIdentifier(options.function);

    let sql = options.orReplace ? 'CREATE OR REPLACE TRIGGER ' : 'CREATE TRIGGER ';
    sql += `${safeName} ${timing} ${events} ON ${safeTable} `;
    sql += forEachRow ? 'FOR EACH ROW ' : 'FOR EACH STATEMENT ';

    if (options.when) {
      sql += `WHEN (${options.when}) `;
    }

    sql += `EXECUTE FUNCTION ${safeFunction}(`;
    if (options.arguments && options.arguments.length > 0) {
      sql += options.arguments.map(arg => this._formatValue(arg)).join(', ');
    }
    sql += ')';

    await this.exec(sql);
  }

  /**
   * Drop a trigger.
   * @param {string} name - Trigger name
   * @param {string} table - Table the trigger is on
   * @param {boolean} [ifExists=false] - Don't error if trigger doesn't exist
   * @returns {Promise<void>}
   */
  async dropTrigger(name, table, ifExists = false) {
    const safeName = this._escapeIdentifier(name);
    const safeTable = this._escapeIdentifier(table);
    const ifExistsClause = ifExists ? 'IF EXISTS ' : '';
    await this.exec(`DROP TRIGGER ${ifExistsClause}${safeName} ON ${safeTable}`);
  }

  /**
   * Enable or disable a trigger.
   * @param {string} name - Trigger name
   * @param {string} table - Table the trigger is on
   * @param {boolean} [enable=true] - Enable (true) or disable (false) the trigger
   * @returns {Promise<void>}
   */
  async alterTrigger(name, table, enable = true) {
    const safeName = this._escapeIdentifier(name);
    const safeTable = this._escapeIdentifier(table);
    const action = enable ? 'ENABLE' : 'DISABLE';
    await this.exec(`ALTER TRIGGER ${safeName} ON ${safeTable} ${action}`);
  }

  /**
   * List triggers on a table or in a database.
   * @param {Object} [options={}] - Filter options
   * @param {string} [options.table] - Filter by table
   * @param {string} [options.database] - Filter by database
   * @returns {Promise<QueryResult>}
   */
  async listTriggers(options = {}) {
    let sql = 'SHOW TRIGGERS';
    if (options.table) {
      sql += ` ON ${this._escapeIdentifier(options.table)}`;
    } else if (options.database) {
      sql += ` IN ${this._escapeIdentifier(options.database)}`;
    }
    return await this.query(sql);
  }

  // ==========================================
  // Stored Procedures and Functions
  // ==========================================

  /**
   * Call a stored procedure.
   * @param {string} procedure - Procedure name
   * @param {...*} args - Arguments to pass to the procedure
   * @returns {Promise<QueryResult>}
   */
  async call(procedure, ...args) {
    const safeName = this._escapeIdentifier(procedure);
    const formattedArgs = args.map(arg => this._formatValue(arg)).join(', ');
    return await this.query(`CALL ${safeName}(${formattedArgs})`);
  }

  /**
   * Create a stored function.
   * @param {string} name - Function name
   * @param {Array<{name: string, type: string}>} parameters - Function parameters
   * @param {string} returnType - Return type
   * @param {string} body - Function body
   * @param {Object} [options={}] - Function options
   * @param {string} [options.language='plpgsql'] - Function language
   * @param {boolean} [options.orReplace=false] - Replace if exists
   * @param {string} [options.volatility] - IMMUTABLE, STABLE, or VOLATILE
   * @returns {Promise<void>}
   */
  async createFunction(name, parameters, returnType, body, options = {}) {
    const safeName = this._escapeIdentifier(name);
    const language = options.language || 'plpgsql';

    const params = parameters.map(p => `${this._escapeIdentifier(p.name)} ${p.type}`).join(', ');

    let sql = options.orReplace ? 'CREATE OR REPLACE FUNCTION ' : 'CREATE FUNCTION ';
    sql += `${safeName}(${params}) RETURNS ${returnType} `;
    sql += `LANGUAGE ${language} `;

    if (options.volatility) {
      sql += `${options.volatility.toUpperCase()} `;
    }

    sql += `AS $$ ${body} $$`;

    await this.exec(sql);
  }

  /**
   * Create a stored procedure.
   * @param {string} name - Procedure name
   * @param {Array<{name: string, type: string, mode?: string}>} parameters - Procedure parameters (mode: IN, OUT, INOUT)
   * @param {string} body - Procedure body
   * @param {Object} [options={}] - Procedure options
   * @param {string} [options.language='plpgsql'] - Procedure language
   * @param {boolean} [options.orReplace=false] - Replace if exists
   * @returns {Promise<void>}
   */
  async createProcedure(name, parameters, body, options = {}) {
    const safeName = this._escapeIdentifier(name);
    const language = options.language || 'plpgsql';

    const params = parameters.map(p => {
      const mode = p.mode ? `${p.mode.toUpperCase()} ` : '';
      return `${mode}${this._escapeIdentifier(p.name)} ${p.type}`;
    }).join(', ');

    let sql = options.orReplace ? 'CREATE OR REPLACE PROCEDURE ' : 'CREATE PROCEDURE ';
    sql += `${safeName}(${params}) `;
    sql += `LANGUAGE ${language} `;
    sql += `AS $$ ${body} $$`;

    await this.exec(sql);
  }

  /**
   * Drop a function.
   * @param {string} name - Function name
   * @param {Array<string>} [paramTypes] - Parameter types for overload resolution
   * @param {boolean} [ifExists=false] - Don't error if function doesn't exist
   * @returns {Promise<void>}
   */
  async dropFunction(name, paramTypes = null, ifExists = false) {
    const safeName = this._escapeIdentifier(name);
    const ifExistsClause = ifExists ? 'IF EXISTS ' : '';
    const signature = paramTypes ? `(${paramTypes.join(', ')})` : '';
    await this.exec(`DROP FUNCTION ${ifExistsClause}${safeName}${signature}`);
  }

  /**
   * Drop a procedure.
   * @param {string} name - Procedure name
   * @param {Array<string>} [paramTypes] - Parameter types for overload resolution
   * @param {boolean} [ifExists=false] - Don't error if procedure doesn't exist
   * @returns {Promise<void>}
   */
  async dropProcedure(name, paramTypes = null, ifExists = false) {
    const safeName = this._escapeIdentifier(name);
    const ifExistsClause = ifExists ? 'IF EXISTS ' : '';
    const signature = paramTypes ? `(${paramTypes.join(', ')})` : '';
    await this.exec(`DROP PROCEDURE ${ifExistsClause}${safeName}${signature}`);
  }

  // ==========================================
  // JSON Operations
  // ==========================================

  /**
   * Extract a JSON value from a column using -> operator.
   * @param {string} column - JSON column name
   * @param {string|number} path - JSON key or array index
   * @param {string} table - Table name
   * @param {Object} [options={}] - Query options
   * @param {string} [options.where] - WHERE clause
   * @param {number} [options.limit] - LIMIT clause
   * @returns {Promise<QueryResult>}
   */
  async jsonExtract(column, path, table, options = {}) {
    const safeColumn = this._escapeIdentifier(column);
    const safeTable = this._escapeIdentifier(table);
    const pathExpr = typeof path === 'number' ? path : `'${this._escapeString(path)}'`;

    let sql = `SELECT ${safeColumn} -> ${pathExpr} AS value FROM ${safeTable}`;
    if (options.where) sql += ` WHERE ${options.where}`;
    if (options.limit) sql += ` LIMIT ${options.limit}`;

    return await this.query(sql);
  }

  /**
   * Extract a JSON value as text using ->> operator.
   * @param {string} column - JSON column name
   * @param {string|number} path - JSON key or array index
   * @param {string} table - Table name
   * @param {Object} [options={}] - Query options
   * @param {string} [options.where] - WHERE clause
   * @param {number} [options.limit] - LIMIT clause
   * @returns {Promise<QueryResult>}
   */
  async jsonExtractText(column, path, table, options = {}) {
    const safeColumn = this._escapeIdentifier(column);
    const safeTable = this._escapeIdentifier(table);
    const pathExpr = typeof path === 'number' ? path : `'${this._escapeString(path)}'`;

    let sql = `SELECT ${safeColumn} ->> ${pathExpr} AS value FROM ${safeTable}`;
    if (options.where) sql += ` WHERE ${options.where}`;
    if (options.limit) sql += ` LIMIT ${options.limit}`;

    return await this.query(sql);
  }

  /**
   * Extract a nested JSON value using #> operator (path array).
   * @param {string} column - JSON column name
   * @param {Array<string>} path - Array of path elements
   * @param {string} table - Table name
   * @param {Object} [options={}] - Query options
   * @returns {Promise<QueryResult>}
   */
  async jsonExtractPath(column, path, table, options = {}) {
    const safeColumn = this._escapeIdentifier(column);
    const safeTable = this._escapeIdentifier(table);
    const pathArray = `'{${path.map(p => this._escapeString(p)).join(',')}}'`;

    let sql = `SELECT ${safeColumn} #> ${pathArray} AS value FROM ${safeTable}`;
    if (options.where) sql += ` WHERE ${options.where}`;
    if (options.limit) sql += ` LIMIT ${options.limit}`;

    return await this.query(sql);
  }

  /**
   * Extract a nested JSON value as text using #>> operator.
   * @param {string} column - JSON column name
   * @param {Array<string>} path - Array of path elements
   * @param {string} table - Table name
   * @param {Object} [options={}] - Query options
   * @returns {Promise<QueryResult>}
   */
  async jsonExtractPathText(column, path, table, options = {}) {
    const safeColumn = this._escapeIdentifier(column);
    const safeTable = this._escapeIdentifier(table);
    const pathArray = `'{${path.map(p => this._escapeString(p)).join(',')}}'`;

    let sql = `SELECT ${safeColumn} #>> ${pathArray} AS value FROM ${safeTable}`;
    if (options.where) sql += ` WHERE ${options.where}`;
    if (options.limit) sql += ` LIMIT ${options.limit}`;

    return await this.query(sql);
  }

  /**
   * Check if JSON contains another JSON value using @> operator.
   * @param {string} column - JSON column name
   * @param {Object} value - JSON value to check for
   * @param {string} table - Table name
   * @param {Object} [options={}] - Query options
   * @returns {Promise<QueryResult>}
   */
  async jsonContains(column, value, table, options = {}) {
    const safeColumn = this._escapeIdentifier(column);
    const safeTable = this._escapeIdentifier(table);
    const jsonValue = JSON.stringify(value);

    let sql = `SELECT * FROM ${safeTable} WHERE ${safeColumn} @> '${this._escapeString(jsonValue)}'::jsonb`;
    if (options.where) sql += ` AND ${options.where}`;
    if (options.limit) sql += ` LIMIT ${options.limit}`;

    return await this.query(sql);
  }

  /**
   * Check if JSON is contained in another JSON value using <@ operator.
   * @param {string} column - JSON column name
   * @param {Object} value - JSON value to check against
   * @param {string} table - Table name
   * @param {Object} [options={}] - Query options
   * @returns {Promise<QueryResult>}
   */
  async jsonContainedBy(column, value, table, options = {}) {
    const safeColumn = this._escapeIdentifier(column);
    const safeTable = this._escapeIdentifier(table);
    const jsonValue = JSON.stringify(value);

    let sql = `SELECT * FROM ${safeTable} WHERE ${safeColumn} <@ '${this._escapeString(jsonValue)}'::jsonb`;
    if (options.where) sql += ` AND ${options.where}`;
    if (options.limit) sql += ` LIMIT ${options.limit}`;

    return await this.query(sql);
  }

  /**
   * Check if a JSON path exists using @? operator.
   * @param {string} column - JSON column name
   * @param {string} jsonPath - JSONPath expression
   * @param {string} table - Table name
   * @param {Object} [options={}] - Query options
   * @returns {Promise<QueryResult>}
   */
  async jsonPathExists(column, jsonPath, table, options = {}) {
    const safeColumn = this._escapeIdentifier(column);
    const safeTable = this._escapeIdentifier(table);

    let sql = `SELECT * FROM ${safeTable} WHERE ${safeColumn} @? '${this._escapeString(jsonPath)}'`;
    if (options.where) sql += ` AND ${options.where}`;
    if (options.limit) sql += ` LIMIT ${options.limit}`;

    return await this.query(sql);
  }

  /**
   * Check if a JSON path predicate matches using @@ operator.
   * @param {string} column - JSON column name
   * @param {string} jsonPath - JSONPath predicate expression
   * @param {string} table - Table name
   * @param {Object} [options={}] - Query options
   * @returns {Promise<QueryResult>}
   */
  async jsonPathMatch(column, jsonPath, table, options = {}) {
    const safeColumn = this._escapeIdentifier(column);
    const safeTable = this._escapeIdentifier(table);

    let sql = `SELECT * FROM ${safeTable} WHERE ${safeColumn} @@ '${this._escapeString(jsonPath)}'`;
    if (options.where) sql += ` AND ${options.where}`;
    if (options.limit) sql += ` LIMIT ${options.limit}`;

    return await this.query(sql);
  }

  // ==========================================
  // Cursor Support
  // ==========================================

  /**
   * Declare a server-side cursor.
   * @param {string} name - Cursor name
   * @param {string} query - SQL query for the cursor
   * @param {Object} [options={}] - Cursor options
   * @param {boolean} [options.scroll=false] - Allow backward movement
   * @param {boolean} [options.hold=false] - Keep cursor open after commit
   * @returns {Promise<void>}
   */
  async declareCursor(name, query, options = {}) {
    const safeName = this._escapeIdentifier(name);
    let sql = 'DECLARE ' + safeName;
    if (options.scroll) sql += ' SCROLL';
    if (options.hold) sql += ' WITH HOLD';
    sql += ' CURSOR FOR ' + query;
    await this.exec(sql);
  }

  /**
   * Fetch rows from a cursor.
   * @param {string} name - Cursor name
   * @param {number} [count=1] - Number of rows to fetch
   * @param {string} [direction='NEXT'] - Fetch direction (NEXT, PRIOR, FIRST, LAST, ABSOLUTE n, RELATIVE n, FORWARD, BACKWARD)
   * @returns {Promise<QueryResult>}
   */
  async fetchCursor(name, count = 1, direction = 'NEXT') {
    const safeName = this._escapeIdentifier(name);
    let sql;
    const dir = direction.toUpperCase();
    if (dir === 'NEXT' || dir === 'PRIOR' || dir === 'FIRST' || dir === 'LAST') {
      sql = `FETCH ${dir} ${count} FROM ${safeName}`;
    } else if (dir.startsWith('ABSOLUTE') || dir.startsWith('RELATIVE')) {
      sql = `FETCH ${dir} FROM ${safeName}`;
    } else {
      sql = `FETCH ${dir} ${count} FROM ${safeName}`;
    }
    return await this.query(sql);
  }

  /**
   * Move cursor position without returning data.
   * @param {string} name - Cursor name
   * @param {number} [count=1] - Number of rows to move
   * @param {string} [direction='NEXT'] - Move direction
   * @returns {Promise<void>}
   */
  async moveCursor(name, count = 1, direction = 'NEXT') {
    const safeName = this._escapeIdentifier(name);
    const dir = direction.toUpperCase();
    let sql;
    if (dir === 'NEXT' || dir === 'PRIOR' || dir === 'FIRST' || dir === 'LAST') {
      sql = `MOVE ${dir} ${count} IN ${safeName}`;
    } else {
      sql = `MOVE ${dir} IN ${safeName}`;
    }
    await this.exec(sql);
  }

  /**
   * Close a cursor.
   * @param {string} name - Cursor name (use '*' or 'ALL' to close all)
   * @returns {Promise<void>}
   */
  async closeCursor(name) {
    if (name === '*' || name.toUpperCase() === 'ALL') {
      await this.exec('CLOSE ALL');
    } else {
      const safeName = this._escapeIdentifier(name);
      await this.exec(`CLOSE ${safeName}`);
    }
  }

  // ==========================================
  // Large Objects (LOB) Support
  // ==========================================

  /**
   * Create a new large object.
   * @returns {Promise<number>} - OID of the new large object
   */
  async loCreate() {
    const result = await this.query('SELECT lo_create(0) AS oid');
    return result.rows[0].oid;
  }

  /**
   * Open a large object.
   * @param {number} oid - OID of the large object
   * @param {string} [mode='rw'] - Open mode ('r', 'w', 'rw')
   * @returns {Promise<number>} - File descriptor
   */
  async loOpen(oid, mode = 'rw') {
    // Mode: INV_READ = 0x40000, INV_WRITE = 0x20000
    let modeFlag = 0x40000; // read
    if (mode.includes('w')) modeFlag |= 0x20000;
    const result = await this.query(`SELECT lo_open(${oid}, ${modeFlag}) AS fd`);
    return result.rows[0].fd;
  }

  /**
   * Write data to a large object.
   * @param {number} fd - File descriptor from loOpen
   * @param {Buffer|string} data - Data to write
   * @returns {Promise<number>} - Number of bytes written
   */
  async loWrite(fd, data) {
    const buffer = Buffer.isBuffer(data) ? data : Buffer.from(data);
    const b64 = buffer.toString('base64');
    const result = await this.query(`SELECT lowrite(${fd}, decode('${b64}', 'base64')) AS written`);
    return result.rows[0].written;
  }

  /**
   * Read data from a large object.
   * @param {number} fd - File descriptor from loOpen
   * @param {number} length - Number of bytes to read
   * @returns {Promise<Buffer>} - Data read
   */
  async loRead(fd, length) {
    const result = await this.query(`SELECT encode(loread(${fd}, ${length}), 'base64') AS data`);
    return Buffer.from(result.rows[0].data, 'base64');
  }

  /**
   * Seek within a large object.
   * @param {number} fd - File descriptor
   * @param {number} offset - Byte offset
   * @param {number} [whence=0] - 0=SEEK_SET, 1=SEEK_CUR, 2=SEEK_END
   * @returns {Promise<number>} - New position
   */
  async loSeek(fd, offset, whence = 0) {
    const result = await this.query(`SELECT lo_lseek(${fd}, ${offset}, ${whence}) AS pos`);
    return result.rows[0].pos;
  }

  /**
   * Get current position in large object.
   * @param {number} fd - File descriptor
   * @returns {Promise<number>} - Current position
   */
  async loTell(fd) {
    const result = await this.query(`SELECT lo_tell(${fd}) AS pos`);
    return result.rows[0].pos;
  }

  /**
   * Close a large object.
   * @param {number} fd - File descriptor
   * @returns {Promise<void>}
   */
  async loClose(fd) {
    await this.query(`SELECT lo_close(${fd})`);
  }

  /**
   * Delete a large object.
   * @param {number} oid - OID of the large object
   * @returns {Promise<void>}
   */
  async loUnlink(oid) {
    await this.query(`SELECT lo_unlink(${oid})`);
  }

  /**
   * Import data as a large object.
   * @param {Buffer|string} data - Data to import
   * @returns {Promise<number>} - OID of the new large object
   */
  async loImport(data) {
    const oid = await this.loCreate();
    const fd = await this.loOpen(oid, 'w');
    try {
      await this.loWrite(fd, data);
    } finally {
      await this.loClose(fd);
    }
    return oid;
  }

  /**
   * Export a large object as data.
   * @param {number} oid - OID of the large object
   * @returns {Promise<Buffer>} - Large object data
   */
  async loExport(oid) {
    const fd = await this.loOpen(oid, 'r');
    const chunks = [];
    try {
      while (true) {
        const chunk = await this.loRead(fd, 8192);
        if (chunk.length === 0) break;
        chunks.push(chunk);
      }
    } finally {
      await this.loClose(fd);
    }
    return Buffer.concat(chunks);
  }

  // ==========================================
  // Advisory Locks
  // ==========================================

  /**
   * Acquire an advisory lock (session-level).
   * @param {number} key - Lock key
   * @param {boolean} [shared=false] - Shared (true) or exclusive (false) lock
   * @returns {Promise<boolean>} - True if lock acquired
   */
  async advisoryLock(key, shared = false) {
    const func = shared ? 'pg_advisory_lock_shared' : 'pg_advisory_lock';
    await this.query(`SELECT ${func}(${key})`);
    return true;
  }

  /**
   * Try to acquire an advisory lock without blocking.
   * @param {number} key - Lock key
   * @param {boolean} [shared=false] - Shared (true) or exclusive (false) lock
   * @returns {Promise<boolean>} - True if lock acquired, false otherwise
   */
  async advisoryLockTry(key, shared = false) {
    const func = shared ? 'pg_try_advisory_lock_shared' : 'pg_try_advisory_lock';
    const result = await this.query(`SELECT ${func}(${key}) AS acquired`);
    return result.rows[0].acquired === true;
  }

  /**
   * Release an advisory lock.
   * @param {number} key - Lock key
   * @param {boolean} [shared=false] - Shared (true) or exclusive (false) lock
   * @returns {Promise<boolean>} - True if lock was released
   */
  async advisoryUnlock(key, shared = false) {
    const func = shared ? 'pg_advisory_unlock_shared' : 'pg_advisory_unlock';
    const result = await this.query(`SELECT ${func}(${key}) AS released`);
    return result.rows[0].released === true;
  }

  /**
   * Release all session-level advisory locks.
   * @returns {Promise<void>}
   */
  async advisoryUnlockAll() {
    await this.query('SELECT pg_advisory_unlock_all()');
  }

  /**
   * Acquire a transaction-level advisory lock.
   * @param {number} key - Lock key
   * @param {boolean} [shared=false] - Shared (true) or exclusive (false) lock
   * @returns {Promise<void>}
   */
  async advisoryXactLock(key, shared = false) {
    const func = shared ? 'pg_advisory_xact_lock_shared' : 'pg_advisory_xact_lock';
    await this.query(`SELECT ${func}(${key})`);
  }

  /**
   * Try to acquire a transaction-level advisory lock without blocking.
   * @param {number} key - Lock key
   * @param {boolean} [shared=false] - Shared (true) or exclusive (false) lock
   * @returns {Promise<boolean>} - True if lock acquired
   */
  async advisoryXactLockTry(key, shared = false) {
    const func = shared ? 'pg_try_advisory_xact_lock_shared' : 'pg_try_advisory_xact_lock';
    const result = await this.query(`SELECT ${func}(${key}) AS acquired`);
    return result.rows[0].acquired === true;
  }

  // ==========================================
  // Full-Text Search
  // ==========================================

  /**
   * Perform a full-text search.
   * @param {string} table - Table name
   * @param {string} textColumn - Text column to search
   * @param {string} query - Search query
   * @param {Object} [options={}] - Search options
   * @param {string} [options.config='english'] - Text search configuration
   * @param {number} [options.limit=100] - Maximum results
   * @param {boolean} [options.rank=true] - Include ranking
   * @param {string} [options.where] - Additional WHERE clause
   * @returns {Promise<QueryResult>}
   */
  async ftsSearch(table, textColumn, query, options = {}) {
    const safeTable = this._escapeIdentifier(table);
    const safeColumn = this._escapeIdentifier(textColumn);
    const config = options.config || 'english';
    const limit = options.limit || 100;
    const safeQuery = this._escapeString(query);

    let sql;
    if (options.rank !== false) {
      sql = `SELECT *, ts_rank(to_tsvector('${config}', ${safeColumn}), plainto_tsquery('${config}', '${safeQuery}')) AS rank ` +
            `FROM ${safeTable} ` +
            `WHERE to_tsvector('${config}', ${safeColumn}) @@ plainto_tsquery('${config}', '${safeQuery}')`;
      if (options.where) sql += ` AND ${options.where}`;
      sql += ` ORDER BY rank DESC LIMIT ${limit}`;
    } else {
      sql = `SELECT * FROM ${safeTable} ` +
            `WHERE to_tsvector('${config}', ${safeColumn}) @@ plainto_tsquery('${config}', '${safeQuery}')`;
      if (options.where) sql += ` AND ${options.where}`;
      sql += ` LIMIT ${limit}`;
    }

    return await this.query(sql);
  }

  /**
   * Get highlighted search results.
   * @param {string} table - Table name
   * @param {string} textColumn - Text column to search
   * @param {string} query - Search query
   * @param {Object} [options={}] - Highlight options
   * @param {string} [options.config='english'] - Text search configuration
   * @param {string} [options.startTag='<b>'] - Tag before match
   * @param {string} [options.stopTag='</b>'] - Tag after match
   * @param {number} [options.limit=100] - Maximum results
   * @returns {Promise<QueryResult>}
   */
  async ftsHighlight(table, textColumn, query, options = {}) {
    const safeTable = this._escapeIdentifier(table);
    const safeColumn = this._escapeIdentifier(textColumn);
    const config = options.config || 'english';
    const startTag = options.startTag || '<b>';
    const stopTag = options.stopTag || '</b>';
    const limit = options.limit || 100;
    const safeQuery = this._escapeString(query);

    const sql = `SELECT *, ts_headline('${config}', ${safeColumn}, plainto_tsquery('${config}', '${safeQuery}'), ` +
                `'StartSel=${startTag}, StopSel=${stopTag}') AS headline ` +
                `FROM ${safeTable} ` +
                `WHERE to_tsvector('${config}', ${safeColumn}) @@ plainto_tsquery('${config}', '${safeQuery}') ` +
                `LIMIT ${limit}`;

    return await this.query(sql);
  }

  // ==========================================
  // Geospatial Support
  // ==========================================

  /**
   * Find records within a distance from a point.
   * @param {string} table - Table name
   * @param {string} geoColumn - Geometry/geography column
   * @param {number} lat - Latitude
   * @param {number} lon - Longitude
   * @param {number} radiusMeters - Search radius in meters
   * @param {Object} [options={}] - Options
   * @param {number} [options.limit=100] - Maximum results
   * @param {string} [options.where] - Additional WHERE clause
   * @returns {Promise<QueryResult>}
   */
  async geoDistance(table, geoColumn, lat, lon, radiusMeters, options = {}) {
    const safeTable = this._escapeIdentifier(table);
    const safeColumn = this._escapeIdentifier(geoColumn);
    const limit = options.limit || 100;

    let sql = `SELECT *, ST_Distance(${safeColumn}, ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326)::geography) AS distance ` +
              `FROM ${safeTable} ` +
              `WHERE ST_DWithin(${safeColumn}, ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326)::geography, ${radiusMeters})`;
    if (options.where) sql += ` AND ${options.where}`;
    sql += ` ORDER BY distance ASC LIMIT ${limit}`;

    return await this.query(sql);
  }

  /**
   * Find records where geometry contains a point or is contained by a polygon.
   * @param {string} table - Table name
   * @param {string} geoColumn - Geometry column
   * @param {string} wkt - WKT (Well-Known Text) of geometry to test
   * @param {Object} [options={}] - Options
   * @param {number} [options.limit=100] - Maximum results
   * @returns {Promise<QueryResult>}
   */
  async geoContains(table, geoColumn, wkt, options = {}) {
    const safeTable = this._escapeIdentifier(table);
    const safeColumn = this._escapeIdentifier(geoColumn);
    const limit = options.limit || 100;

    const sql = `SELECT * FROM ${safeTable} ` +
                `WHERE ST_Contains(${safeColumn}, ST_GeomFromText('${this._escapeString(wkt)}', 4326)) ` +
                `LIMIT ${limit}`;

    return await this.query(sql);
  }

  /**
   * Find K nearest neighbors to a point.
   * @param {string} table - Table name
   * @param {string} geoColumn - Geometry/geography column
   * @param {number} lat - Latitude
   * @param {number} lon - Longitude
   * @param {number} [k=10] - Number of neighbors
   * @param {Object} [options={}] - Options
   * @returns {Promise<QueryResult>}
   */
  async geoNearest(table, geoColumn, lat, lon, k = 10, options = {}) {
    const safeTable = this._escapeIdentifier(table);
    const safeColumn = this._escapeIdentifier(geoColumn);

    let sql = `SELECT *, ST_Distance(${safeColumn}, ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326)::geography) AS distance ` +
              `FROM ${safeTable}`;
    if (options.where) sql += ` WHERE ${options.where}`;
    sql += ` ORDER BY ${safeColumn} <-> ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326)::geometry LIMIT ${k}`;

    return await this.query(sql);
  }

  /**
   * Find records that intersect with a geometry.
   * @param {string} table - Table name
   * @param {string} geoColumn - Geometry column
   * @param {string} wkt - WKT of geometry to intersect with
   * @param {Object} [options={}] - Options
   * @returns {Promise<QueryResult>}
   */
  async geoIntersects(table, geoColumn, wkt, options = {}) {
    const safeTable = this._escapeIdentifier(table);
    const safeColumn = this._escapeIdentifier(geoColumn);
    const limit = options.limit || 100;

    const sql = `SELECT * FROM ${safeTable} ` +
                `WHERE ST_Intersects(${safeColumn}, ST_GeomFromText('${this._escapeString(wkt)}', 4326)) ` +
                `LIMIT ${limit}`;

    return await this.query(sql);
  }

  // ==========================================
  // Time Travel Queries
  // ==========================================

  /**
   * Query data as of a specific timestamp.
   * @param {string} sql - SQL query
   * @param {Date|string} timestamp - Point in time
   * @param {Object} [options={}] - Options
   * @returns {Promise<QueryResult>}
   */
  async queryAsOf(sql, timestamp, options = {}) {
    const ts = timestamp instanceof Date ? timestamp.toISOString() : timestamp;
    const modifiedSql = `${sql} AS OF TIMESTAMP '${this._escapeString(ts)}'`;
    return await this.query(modifiedSql, options);
  }

  /**
   * Query data changes between two timestamps.
   * @param {string} sql - SQL query
   * @param {Date|string} startTime - Start timestamp
   * @param {Date|string} endTime - End timestamp
   * @param {Object} [options={}] - Options
   * @returns {Promise<QueryResult>}
   */
  async queryBetween(sql, startTime, endTime, options = {}) {
    const start = startTime instanceof Date ? startTime.toISOString() : startTime;
    const end = endTime instanceof Date ? endTime.toISOString() : endTime;
    const modifiedSql = `${sql} FOR SYSTEM_TIME FROM '${this._escapeString(start)}' TO '${this._escapeString(end)}'`;
    return await this.query(modifiedSql, options);
  }

  /**
   * Get the history of a table.
   * @param {string} table - Table name
   * @param {Object} [options={}] - Options
   * @param {Date|string} [options.since] - Start timestamp
   * @param {Date|string} [options.until] - End timestamp
   * @param {number} [options.limit=1000] - Maximum rows
   * @returns {Promise<QueryResult>}
   */
  async getHistory(table, options = {}) {
    const safeTable = this._escapeIdentifier(table);
    const limit = options.limit || 1000;

    let sql = `SELECT * FROM ${safeTable}`;
    if (options.since && options.until) {
      const since = options.since instanceof Date ? options.since.toISOString() : options.since;
      const until = options.until instanceof Date ? options.until.toISOString() : options.until;
      sql += ` FOR SYSTEM_TIME FROM '${this._escapeString(since)}' TO '${this._escapeString(until)}'`;
    } else if (options.since) {
      const since = options.since instanceof Date ? options.since.toISOString() : options.since;
      sql += ` FOR SYSTEM_TIME FROM '${this._escapeString(since)}' TO NOW()`;
    }
    sql += ` LIMIT ${limit}`;

    return await this.query(sql);
  }

  // ==========================================
  // Batch Operations
  // ==========================================

  /**
   * Insert multiple rows in batches.
   * @param {string} table - Table name
   * @param {Array<Object>} rows - Rows to insert
   * @param {Object} [options={}] - Options
   * @param {number} [options.chunkSize=1000] - Batch size
   * @returns {Promise<number>} - Total rows inserted
   */
  async batchInsert(table, rows, options = {}) {
    if (!rows || rows.length === 0) return 0;

    const chunkSize = options.chunkSize || 1000;
    const safeTable = this._escapeIdentifier(table);
    const columns = Object.keys(rows[0]);
    const safeColumns = columns.map(c => this._escapeIdentifier(c)).join(', ');

    let totalInserted = 0;
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      const values = chunk.map(row => {
        const vals = columns.map(c => this._formatValue(row[c]));
        return `(${vals.join(', ')})`;
      }).join(', ');

      const sql = `INSERT INTO ${safeTable} (${safeColumns}) VALUES ${values}`;
      await this.exec(sql);
      totalInserted += chunk.length;
    }

    return totalInserted;
  }

  /**
   * Update multiple rows by key.
   * @param {string} table - Table name
   * @param {Array<Object>} updates - Updates, each containing key and fields to update
   * @param {string} keyColumn - Primary key column name
   * @param {Object} [options={}] - Options
   * @returns {Promise<number>} - Total rows updated
   */
  async batchUpdate(table, updates, keyColumn, options = {}) {
    if (!updates || updates.length === 0) return 0;

    const safeTable = this._escapeIdentifier(table);
    const safeKeyColumn = this._escapeIdentifier(keyColumn);
    let totalUpdated = 0;

    for (const update of updates) {
      const keyValue = update[keyColumn];
      const setClauses = Object.entries(update)
        .filter(([k]) => k !== keyColumn)
        .map(([k, v]) => `${this._escapeIdentifier(k)} = ${this._formatValue(v)}`)
        .join(', ');

      if (!setClauses) continue;

      const sql = `UPDATE ${safeTable} SET ${setClauses} WHERE ${safeKeyColumn} = ${this._formatValue(keyValue)}`;
      await this.exec(sql);
      totalUpdated++;
    }

    return totalUpdated;
  }

  /**
   * Upsert multiple rows (INSERT ... ON CONFLICT).
   * @param {string} table - Table name
   * @param {Array<Object>} rows - Rows to upsert
   * @param {Array<string>} conflictColumns - Columns that define the conflict
   * @param {Array<string>} [updateColumns] - Columns to update on conflict (defaults to all non-conflict columns)
   * @param {Object} [options={}] - Options
   * @param {number} [options.chunkSize=1000] - Batch size
   * @returns {Promise<number>} - Total rows affected
   */
  async batchUpsert(table, rows, conflictColumns, updateColumns = null, options = {}) {
    if (!rows || rows.length === 0) return 0;

    const chunkSize = options.chunkSize || 1000;
    const safeTable = this._escapeIdentifier(table);
    const columns = Object.keys(rows[0]);
    const safeColumns = columns.map(c => this._escapeIdentifier(c)).join(', ');
    const safeConflict = conflictColumns.map(c => this._escapeIdentifier(c)).join(', ');

    const updateCols = updateColumns || columns.filter(c => !conflictColumns.includes(c));
    const updateClause = updateCols
      .map(c => `${this._escapeIdentifier(c)} = EXCLUDED.${this._escapeIdentifier(c)}`)
      .join(', ');

    let totalAffected = 0;
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      const values = chunk.map(row => {
        const vals = columns.map(c => this._formatValue(row[c]));
        return `(${vals.join(', ')})`;
      }).join(', ');

      let sql = `INSERT INTO ${safeTable} (${safeColumns}) VALUES ${values} ` +
                `ON CONFLICT (${safeConflict})`;
      if (updateClause) {
        sql += ` DO UPDATE SET ${updateClause}`;
      } else {
        sql += ' DO NOTHING';
      }

      await this.exec(sql);
      totalAffected += chunk.length;
    }

    return totalAffected;
  }

  /**
   * Delete multiple rows by key values.
   * @param {string} table - Table name
   * @param {Array} keys - Key values to delete
   * @param {string} keyColumn - Key column name
   * @param {Object} [options={}] - Options
   * @param {number} [options.chunkSize=1000] - Batch size
   * @returns {Promise<number>} - Total rows deleted
   */
  async batchDelete(table, keys, keyColumn, options = {}) {
    if (!keys || keys.length === 0) return 0;

    const chunkSize = options.chunkSize || 1000;
    const safeTable = this._escapeIdentifier(table);
    const safeKeyColumn = this._escapeIdentifier(keyColumn);

    let totalDeleted = 0;
    for (let i = 0; i < keys.length; i += chunkSize) {
      const chunk = keys.slice(i, i + chunkSize);
      const values = chunk.map(k => this._formatValue(k)).join(', ');

      const sql = `DELETE FROM ${safeTable} WHERE ${safeKeyColumn} IN (${values})`;
      await this.exec(sql);
      totalDeleted += chunk.length;
    }

    return totalDeleted;
  }

  // ==========================================
  // Schema Introspection
  // ==========================================

  /**
   * Get column information for a table.
   * @param {string} table - Table name
   * @param {string} [database] - Database name
   * @returns {Promise<QueryResult>}
   */
  async getColumns(table, database = null) {
    const db = database || this.config.database;
    const safeTable = this._escapeIdentifier(table);
    let sql;
    if (db) {
      sql = `DESCRIBE ${this._escapeIdentifier(db)}.${safeTable}`;
    } else {
      sql = `DESCRIBE ${safeTable}`;
    }
    return await this.query(sql);
  }

  /**
   * Get indexes for a table.
   * @param {string} table - Table name
   * @param {string} [database] - Database name
   * @returns {Promise<QueryResult>}
   */
  async getIndexes(table, database = null) {
    const db = database || this.config.database;
    const safeTable = this._escapeIdentifier(table);
    let sql;
    if (db) {
      sql = `SHOW INDEXES ON ${this._escapeIdentifier(db)}.${safeTable}`;
    } else {
      sql = `SHOW INDEXES ON ${safeTable}`;
    }
    return await this.query(sql);
  }

  /**
   * Get constraints for a table.
   * @param {string} table - Table name
   * @param {string} [database] - Database name
   * @returns {Promise<QueryResult>}
   */
  async getConstraints(table, database = null) {
    const db = database || this.config.database;
    const safeTable = this._escapeIdentifier(table);
    let sql;
    if (db) {
      sql = `SHOW CONSTRAINTS ON ${this._escapeIdentifier(db)}.${safeTable}`;
    } else {
      sql = `SHOW CONSTRAINTS ON ${safeTable}`;
    }
    return await this.query(sql);
  }

  /**
   * Get table statistics.
   * @param {string} table - Table name
   * @param {string} [database] - Database name
   * @returns {Promise<QueryResult>}
   */
  async getTableStats(table, database = null) {
    const db = database || this.config.database;
    const safeTable = this._escapeIdentifier(table);
    let sql;
    if (db) {
      sql = `SHOW STATS FOR ${this._escapeIdentifier(db)}.${safeTable}`;
    } else {
      sql = `SHOW STATS FOR ${safeTable}`;
    }
    return await this.query(sql);
  }

  /**
   * Get full schema for a database.
   * @param {string} [database] - Database name
   * @returns {Promise<{tables: Array, indexes: Array, constraints: Array}>}
   */
  async getSchema(database = null) {
    const db = database || this.config.database;
    const tables = await this.listTables(db);

    const schema = {
      tables: [],
      indexes: [],
      constraints: [],
    };

    for (const tableInfo of tables) {
      const tableName = tableInfo.name || tableInfo;
      try {
        const columns = await this.getColumns(tableName, db);
        schema.tables.push({ name: tableName, columns: columns.rows });

        try {
          const indexes = await this.getIndexes(tableName, db);
          schema.indexes.push(...indexes.rows.map(idx => ({ ...idx, table: tableName })));
        } catch (e) { /* No indexes */ }

        try {
          const constraints = await this.getConstraints(tableName, db);
          schema.constraints.push(...constraints.rows.map(c => ({ ...c, table: tableName })));
        } catch (e) { /* No constraints */ }
      } catch (e) {
        schema.tables.push({ name: tableName, error: e.message });
      }
    }

    return schema;
  }

  // ==========================================
  // Array Operations
  // ==========================================

  /**
   * Find rows where array column contains a value.
   * @param {string} table - Table name
   * @param {string} arrayColumn - Array column name
   * @param {*} value - Value to search for
   * @param {Object} [options={}] - Options
   * @returns {Promise<QueryResult>}
   */
  async arrayContains(table, arrayColumn, value, options = {}) {
    const safeTable = this._escapeIdentifier(table);
    const safeColumn = this._escapeIdentifier(arrayColumn);
    const limit = options.limit || 100;

    let sql = `SELECT * FROM ${safeTable} WHERE ${this._formatValue(value)} = ANY(${safeColumn})`;
    if (options.where) sql += ` AND ${options.where}`;
    sql += ` LIMIT ${limit}`;

    return await this.query(sql);
  }

  /**
   * Find rows where array columns overlap.
   * @param {string} table - Table name
   * @param {string} arrayColumn - Array column name
   * @param {Array} values - Values to check for overlap
   * @param {Object} [options={}] - Options
   * @returns {Promise<QueryResult>}
   */
  async arrayOverlap(table, arrayColumn, values, options = {}) {
    const safeTable = this._escapeIdentifier(table);
    const safeColumn = this._escapeIdentifier(arrayColumn);
    const limit = options.limit || 100;
    const arrayLiteral = `ARRAY[${values.map(v => this._formatValue(v)).join(', ')}]`;

    let sql = `SELECT * FROM ${safeTable} WHERE ${safeColumn} && ${arrayLiteral}`;
    if (options.where) sql += ` AND ${options.where}`;
    sql += ` LIMIT ${limit}`;

    return await this.query(sql);
  }

  /**
   * Find rows where array contains all specified values.
   * @param {string} table - Table name
   * @param {string} arrayColumn - Array column name
   * @param {Array} values - Values that must all be present
   * @param {Object} [options={}] - Options
   * @returns {Promise<QueryResult>}
   */
  async arrayContainsAll(table, arrayColumn, values, options = {}) {
    const safeTable = this._escapeIdentifier(table);
    const safeColumn = this._escapeIdentifier(arrayColumn);
    const limit = options.limit || 100;
    const arrayLiteral = `ARRAY[${values.map(v => this._formatValue(v)).join(', ')}]`;

    let sql = `SELECT * FROM ${safeTable} WHERE ${safeColumn} @> ${arrayLiteral}`;
    if (options.where) sql += ` AND ${options.where}`;
    sql += ` LIMIT ${limit}`;

    return await this.query(sql);
  }

  // ==========================================
  // Async Pub/Sub Polling
  // ==========================================

  /**
   * Poll for pending notifications.
   * @param {number} [timeout=0] - Timeout in milliseconds (0 = non-blocking)
   * @returns {Promise<Array<{channel: string, payload: string}>>}
   */
  async pollNotifications(timeout = 0) {
    const result = await this.query(`SELECT * FROM pg_notification_queue(${timeout})`);
    return result.rows.map(row => ({
      channel: row.channel,
      payload: row.payload,
    }));
  }

  // ==========================================
  // Helper Methods
  // ==========================================

  /**
   * Escape an identifier (table name, column name, etc.).
   * @private
   * @param {string} identifier - Identifier to escape
   * @returns {string} - Escaped identifier
   */
  _escapeIdentifier(identifier) {
    // Handle qualified names (database.table)
    if (identifier.includes('.')) {
      return identifier.split('.').map(part => this._escapeIdentifier(part)).join('.');
    }
    // Simple escape - double any double quotes
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  /**
   * Escape a string value for SQL.
   * @private
   * @param {string} value - String to escape
   * @returns {string} - Escaped string (without surrounding quotes)
   */
  _escapeString(value) {
    return String(value).replace(/'/g, "''");
  }

  /**
   * Format a value for SQL.
   * @private
   * @param {*} value - Value to format
   * @returns {string} - Formatted value
   */
  _formatValue(value) {
    if (value === null || value === undefined) {
      return 'NULL';
    }
    if (typeof value === 'number') {
      return String(value);
    }
    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE';
    }
    if (Array.isArray(value) || typeof value === 'object') {
      return `'${this._escapeString(JSON.stringify(value))}'`;
    }
    return `'${this._escapeString(value)}'`;
  }

  /**
   * Execute a function within a transaction.
   * If the function throws, the transaction is rolled back.
   * Otherwise, the transaction is committed.
   * @param {Function} fn - Async function to execute within the transaction
   * @param {Object} [options={}] - Transaction options
   * @returns {Promise<*>} - Result of the function
   */
  async inTransaction(fn, options = {}) {
    await this.begin(options);
    try {
      const result = await fn();
      await this.commit();
      return result;
    } catch (err) {
      try {
        await this.rollback();
      } catch (rollbackErr) {
        // Ignore rollback errors
      }
      throw err;
    }
  }

  // Vector Search Support

  /**
   * Perform a vector similarity search.
   * @param {string} table - Table name (format: database.table or just table if database is set)
   * @param {string} column - Vector column name
   * @param {Array<number>} queryVector - Query vector
   * @param {Object} [options={}] - Search options
   * @param {number} [options.k=10] - Number of results to return
   * @param {string} [options.metric='cosine'] - Distance metric (cosine, euclidean, dot_product, manhattan)
   * @param {string} [options.filter] - SQL WHERE clause for filtering
   * @param {number} [options.efSearch] - HNSW ef_search parameter
   * @param {number} [options.nprobe] - IVF nprobe parameter
   * @returns {Promise<Array<{id: any, distance: number, data: Object}>>}
   */
  async vectorSearch(table, column, queryVector, options = {}) {
    const k = options.k || 10;
    const metric = options.metric || 'cosine';

    // Build the distance function call
    let distanceFunc;
    switch (metric.toLowerCase()) {
      case 'euclidean':
        distanceFunc = 'euclidean_distance';
        break;
      case 'dot_product':
      case 'inner_product':
        distanceFunc = 'inner_product';
        break;
      case 'manhattan':
        distanceFunc = 'manhattan_distance';
        break;
      case 'cosine':
      default:
        distanceFunc = 'vector_similarity';
        break;
    }

    // Format query vector as array literal
    const vectorStr = `[${queryVector.join(',')}]`;

    // Build SQL query
    let sql = `SELECT *, ${distanceFunc}(${column}, ${vectorStr}) AS distance FROM ${table}`;

    if (options.filter) {
      sql += ` WHERE ${options.filter}`;
    }

    // For cosine similarity, higher is better; for distances, lower is better
    if (metric.toLowerCase() === 'cosine') {
      sql += ` ORDER BY distance DESC`;
    } else {
      sql += ` ORDER BY distance ASC`;
    }

    sql += ` LIMIT ${k}`;

    const result = await this.query(sql);

    return result.rows.map(row => ({
      id: row.id || row._id || null,
      distance: row.distance,
      data: row,
    }));
  }

  /**
   * Perform a hybrid search combining vector similarity and text search.
   * @param {string} table - Table name
   * @param {string} vectorColumn - Vector column name
   * @param {Array<number>} queryVector - Query vector
   * @param {string} textColumn - Text column for text search
   * @param {string} textQuery - Text search query
   * @param {Object} [options={}] - Search options
   * @param {number} [options.k=10] - Number of results to return
   * @param {string} [options.metric='cosine'] - Distance metric
   * @param {number} [options.vectorWeight=0.5] - Weight for vector results (0-1)
   * @param {string} [options.fusion='rrf'] - Fusion method (rrf, linear)
   * @param {number} [options.rrfK=60] - RRF k parameter
   * @param {string} [options.filter] - SQL WHERE clause for filtering
   * @returns {Promise<Array<{id: any, score: number, data: Object}>>}
   */
  async hybridSearch(table, vectorColumn, queryVector, textColumn, textQuery, options = {}) {
    const k = options.k || 10;
    const metric = options.metric || 'cosine';
    const vectorWeight = options.vectorWeight !== undefined ? options.vectorWeight : 0.5;
    const textWeight = 1.0 - vectorWeight;
    const fusion = options.fusion || 'rrf';
    const rrfK = options.rrfK || 60;

    // Get vector search results
    const vectorResults = await this.vectorSearch(table, vectorColumn, queryVector, {
      k: k * 2,
      metric,
      filter: options.filter,
    });

    // Get text search results
    let textSql = `SELECT *, 1.0 AS text_score FROM ${table} WHERE ${textColumn} LIKE '%${textQuery.replace(/'/g, "''")}%'`;
    if (options.filter) {
      textSql += ` AND ${options.filter}`;
    }
    textSql += ` LIMIT ${k * 2}`;

    const textResult = await this.query(textSql);
    const textResults = textResult.rows;

    // Build ID maps
    const vectorScores = new Map();
    const textScores = new Map();

    vectorResults.forEach((item, idx) => {
      const id = JSON.stringify(item.id || item.data);
      vectorScores.set(id, { rank: idx + 1, score: item.distance, data: item.data });
    });

    textResults.forEach((item, idx) => {
      const id = JSON.stringify(item.id || item._id || item);
      textScores.set(id, { rank: idx + 1, score: 1.0, data: item });
    });

    // Combine all IDs
    const allIds = new Set([...vectorScores.keys(), ...textScores.keys()]);

    // Calculate fusion scores
    const results = [];
    for (const id of allIds) {
      const vectorItem = vectorScores.get(id);
      const textItem = textScores.get(id);

      let score;
      if (fusion === 'rrf') {
        // Reciprocal Rank Fusion
        const vectorRrf = vectorItem ? 1.0 / (rrfK + vectorItem.rank) : 0;
        const textRrf = textItem ? 1.0 / (rrfK + textItem.rank) : 0;
        score = vectorWeight * vectorRrf + textWeight * textRrf;
      } else {
        // Linear fusion
        const vectorScore = vectorItem ? vectorItem.score : 0;
        const textScore = textItem ? textItem.score : 0;
        score = vectorWeight * vectorScore + textWeight * textScore;
      }

      const data = vectorItem ? vectorItem.data : textItem.data;
      results.push({
        id: vectorItem?.data?.id || textItem?.id || null,
        score,
        data,
      });
    }

    // Sort by score (descending)
    results.sort((a, b) => b.score - a.score);

    return results.slice(0, k);
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

const { PoolConfig, ConnectionPool, PooledClient } = require('./pool');

// Vector Utility Functions

/**
 * Calculate cosine similarity between two vectors.
 * @param {Array<number>} a - First vector
 * @param {Array<number>} b - Second vector
 * @returns {number} - Cosine similarity (-1 to 1)
 */
function cosineSimilarity(a, b) {
  if (a.length !== b.length) {
    throw new Error('Vectors must have the same length');
  }

  let dotProduct = 0;
  let normA = 0;
  let normB = 0;

  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }

  const magnitude = Math.sqrt(normA) * Math.sqrt(normB);
  if (magnitude === 0) {
    return 0;
  }

  return dotProduct / magnitude;
}

/**
 * Calculate Euclidean distance between two vectors.
 * @param {Array<number>} a - First vector
 * @param {Array<number>} b - Second vector
 * @returns {number} - Euclidean distance
 */
function euclideanDistance(a, b) {
  if (a.length !== b.length) {
    throw new Error('Vectors must have the same length');
  }

  let sum = 0;
  for (let i = 0; i < a.length; i++) {
    const diff = a[i] - b[i];
    sum += diff * diff;
  }

  return Math.sqrt(sum);
}

/**
 * Calculate dot product of two vectors.
 * @param {Array<number>} a - First vector
 * @param {Array<number>} b - Second vector
 * @returns {number} - Dot product
 */
function dotProduct(a, b) {
  if (a.length !== b.length) {
    throw new Error('Vectors must have the same length');
  }

  let sum = 0;
  for (let i = 0; i < a.length; i++) {
    sum += a[i] * b[i];
  }

  return sum;
}

/**
 * Calculate Manhattan distance between two vectors.
 * @param {Array<number>} a - First vector
 * @param {Array<number>} b - Second vector
 * @returns {number} - Manhattan distance
 */
function manhattanDistance(a, b) {
  if (a.length !== b.length) {
    throw new Error('Vectors must have the same length');
  }

  let sum = 0;
  for (let i = 0; i < a.length; i++) {
    sum += Math.abs(a[i] - b[i]);
  }

  return sum;
}

/**
 * Normalize a vector to unit length.
 * @param {Array<number>} vector - Input vector
 * @returns {Array<number>} - Normalized vector
 */
function normalizeVector(vector) {
  let norm = 0;
  for (let i = 0; i < vector.length; i++) {
    norm += vector[i] * vector[i];
  }
  norm = Math.sqrt(norm);

  if (norm === 0) {
    return vector.slice();
  }

  return vector.map(v => v / norm);
}

// Text Chunking

/**
 * Chunking strategies for text processing.
 * @enum {string}
 */
const ChunkingStrategy = {
  FIXED_SIZE: 'fixed_size',
  SENTENCE: 'sentence',
  PARAGRAPH: 'paragraph',
  SEMANTIC: 'semantic',
};

/**
 * Chunk text into smaller pieces for embedding.
 * @param {string} text - Text to chunk
 * @param {Object} [options={}] - Chunking options
 * @param {string} [options.strategy='fixed_size'] - Chunking strategy
 * @param {number} [options.chunkSize=512] - Target chunk size in characters
 * @param {number} [options.overlap=50] - Overlap between chunks
 * @returns {Array<{text: string, start: number, end: number}>}
 */
function chunkText(text, options = {}) {
  const strategy = options.strategy || ChunkingStrategy.FIXED_SIZE;
  const chunkSize = options.chunkSize || 512;
  const overlap = options.overlap || 50;

  const chunks = [];

  switch (strategy) {
    case ChunkingStrategy.SENTENCE: {
      // Split by sentence boundaries
      const sentences = text.match(/[^.!?]+[.!?]+/g) || [text];
      let currentChunk = '';
      let currentStart = 0;
      let charPos = 0;

      for (const sentence of sentences) {
        if (currentChunk.length + sentence.length > chunkSize && currentChunk.length > 0) {
          chunks.push({
            text: currentChunk.trim(),
            start: currentStart,
            end: charPos,
          });
          currentChunk = sentence;
          currentStart = charPos;
        } else {
          currentChunk += sentence;
        }
        charPos += sentence.length;
      }

      if (currentChunk.length > 0) {
        chunks.push({
          text: currentChunk.trim(),
          start: currentStart,
          end: charPos,
        });
      }
      break;
    }

    case ChunkingStrategy.PARAGRAPH: {
      // Split by paragraphs
      const paragraphs = text.split(/\n\s*\n/);
      let charPos = 0;

      for (const para of paragraphs) {
        if (para.trim().length > 0) {
          chunks.push({
            text: para.trim(),
            start: charPos,
            end: charPos + para.length,
          });
        }
        charPos += para.length + 2; // Account for newlines
      }
      break;
    }

    case ChunkingStrategy.FIXED_SIZE:
    default: {
      // Fixed size with overlap
      let pos = 0;
      while (pos < text.length) {
        const end = Math.min(pos + chunkSize, text.length);
        chunks.push({
          text: text.slice(pos, end),
          start: pos,
          end: end,
        });
        pos += chunkSize - overlap;
        if (pos + overlap >= text.length) {
          break;
        }
      }
      break;
    }
  }

  return chunks;
}

// Embedding Models

/**
 * Common embedding models with their configurations.
 */
const EmbeddingModels = {
  // OpenAI models
  'text-embedding-3-small': {
    name: 'text-embedding-3-small',
    provider: 'openai',
    dimensions: 1536,
    maxTokens: 8191,
  },
  'text-embedding-3-large': {
    name: 'text-embedding-3-large',
    provider: 'openai',
    dimensions: 3072,
    maxTokens: 8191,
  },
  'text-embedding-ada-002': {
    name: 'text-embedding-ada-002',
    provider: 'openai',
    dimensions: 1536,
    maxTokens: 8191,
  },

  // HuggingFace models
  'all-MiniLM-L6-v2': {
    name: 'sentence-transformers/all-MiniLM-L6-v2',
    provider: 'huggingface',
    dimensions: 384,
    maxTokens: 256,
  },
  'all-mpnet-base-v2': {
    name: 'sentence-transformers/all-mpnet-base-v2',
    provider: 'huggingface',
    dimensions: 768,
    maxTokens: 384,
  },
  'e5-large-v2': {
    name: 'intfloat/e5-large-v2',
    provider: 'huggingface',
    dimensions: 1024,
    maxTokens: 512,
  },
  'bge-large-en-v1.5': {
    name: 'BAAI/bge-large-en-v1.5',
    provider: 'huggingface',
    dimensions: 1024,
    maxTokens: 512,
  },

  // Cohere models
  'embed-english-v3.0': {
    name: 'embed-english-v3.0',
    provider: 'cohere',
    dimensions: 1024,
    maxTokens: 512,
  },
  'embed-multilingual-v3.0': {
    name: 'embed-multilingual-v3.0',
    provider: 'cohere',
    dimensions: 1024,
    maxTokens: 512,
  },
};

/**
 * Get embedding model configuration.
 * @param {string} modelName - Model name
 * @returns {Object|null} - Model configuration or null if not found
 */
function getEmbeddingModel(modelName) {
  return EmbeddingModels[modelName] || null;
}

/**
 * List all available embedding models.
 * @returns {Array<Object>} - Array of model configurations
 */
function listEmbeddingModels() {
  return Object.values(EmbeddingModels);
}

module.exports = {
  Client,
  PoolConfig,
  ConnectionPool,
  PooledClient,

  // Vector utilities
  cosineSimilarity,
  euclideanDistance,
  dotProduct,
  manhattanDistance,
  normalizeVector,

  // Text chunking
  ChunkingStrategy,
  chunkText,

  // Embedding models
  EmbeddingModels,
  getEmbeddingModel,
  listEmbeddingModels,
};
