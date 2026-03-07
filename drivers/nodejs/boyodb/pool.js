/**
 * Connection pooling for boyodb Node.js client.
 *
 * @example
 * const { PooledClient, PoolConfig } = require('boyodb/pool');
 *
 * const config = new PoolConfig({
 *   host: 'localhost',
 *   port: 8765,
 *   poolSize: 20,
 *   database: 'analytics'
 * });
 *
 * const client = new PooledClient(config);
 *
 * // Thread-safe concurrent queries
 * const result = await client.query('SELECT COUNT(*) FROM events');
 * console.log(result);
 *
 * await client.close();
 */

const net = require('net');
const tls = require('tls');
const fs = require('fs');
const { tableFromIPC } = require('apache-arrow');

/**
 * Pool configuration options.
 */
class PoolConfig {
  /**
   * @param {Object} options
   * @param {string} [options.host='localhost'] - Server host
   * @param {number} [options.port=8765] - Server port
   * @param {number} [options.poolSize=10] - Number of connections in pool
   * @param {number} [options.poolTimeout=30000] - Timeout for acquiring connection (ms)
   * @param {boolean} [options.tls=false] - Enable TLS
   * @param {string} [options.caFile] - CA certificate file
   * @param {boolean} [options.insecureSkipVerify=false] - Skip TLS verification
   * @param {number} [options.connectTimeout=10000] - Connection timeout (ms)
   * @param {number} [options.readTimeout=30000] - Read timeout (ms)
   * @param {number} [options.writeTimeout=10000] - Write timeout (ms)
   * @param {string} [options.token] - Auth token
   * @param {number} [options.maxRetries=3] - Max connection retries
   * @param {number} [options.retryDelay=1000] - Retry delay (ms)
   * @param {string} [options.database] - Default database
   * @param {number} [options.queryTimeout=30000] - Query timeout (ms)
   */
  constructor(options = {}) {
    this.host = options.host || 'localhost';
    this.port = options.port || 8765;
    this.poolSize = options.poolSize || 10;
    this.poolTimeout = options.poolTimeout || 30000;
    this.tls = options.tls || false;
    this.caFile = options.caFile || null;
    this.insecureSkipVerify = options.insecureSkipVerify || false;
    this.connectTimeout = options.connectTimeout || 10000;
    this.readTimeout = options.readTimeout || 30000;
    this.writeTimeout = options.writeTimeout || 10000;
    this.token = options.token || null;
    this.maxRetries = options.maxRetries || 3;
    this.retryDelay = options.retryDelay || 1000;
    this.database = options.database || null;
    this.queryTimeout = options.queryTimeout || 30000;
  }
}

/**
 * A pooled connection wrapper.
 */
class PooledConnection {
  constructor(socket) {
    this._socket = socket;
    this._valid = true;
    this._createdAt = Date.now();
  }

  get socket() {
    return this._socket;
  }

  get valid() {
    return this._valid && !this._socket.destroyed;
  }

  invalidate() {
    this._valid = false;
  }

  close() {
    this._valid = false;
    try {
      this._socket.destroy();
    } catch (e) {
      // Ignore
    }
  }
}

/**
 * Thread-safe connection pool for BoyoDB.
 */
class ConnectionPool {
  /**
   * @param {PoolConfig} config
   */
  constructor(config) {
    this._config = config;
    this._pool = [];
    this._waiting = [];
    this._closed = false;
    this._sessionId = null;
    this._initializing = null;
  }

  /**
   * Initialize the pool with connections.
   * @returns {Promise<void>}
   */
  async initialize() {
    if (this._initializing) {
      return this._initializing;
    }

    this._initializing = (async () => {
      const promises = [];
      for (let i = 0; i < this._config.poolSize; i++) {
        promises.push(this._createConnection());
      }
      const connections = await Promise.all(promises);
      this._pool.push(...connections);
    })();

    return this._initializing;
  }

  /**
   * Create a new connection.
   * @private
   * @returns {Promise<PooledConnection>}
   */
  async _createConnection() {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Connection timeout'));
      }, this._config.connectTimeout);

      const options = {
        host: this._config.host,
        port: this._config.port,
      };

      let socket;
      if (this._config.tls) {
        if (this._config.insecureSkipVerify) {
          console.warn('WARNING: TLS certificate verification is DISABLED.');
        }

        const tlsOptions = {
          ...options,
          rejectUnauthorized: !this._config.insecureSkipVerify,
        };

        if (this._config.caFile) {
          tlsOptions.ca = fs.readFileSync(this._config.caFile);
        }

        socket = tls.connect(tlsOptions, () => {
          clearTimeout(timeout);
          resolve(new PooledConnection(socket));
        });
      } else {
        socket = net.connect(options, () => {
          clearTimeout(timeout);
          resolve(new PooledConnection(socket));
        });
      }

      socket.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }

  /**
   * Borrow a connection from the pool.
   * @returns {Promise<PooledConnection>}
   */
  async _borrow() {
    if (this._closed) {
      throw new Error('Pool is closed');
    }

    // Try to get an available connection
    while (this._pool.length > 0) {
      const conn = this._pool.pop();
      if (conn.valid) {
        return conn;
      }
      conn.close();
    }

    // Wait for a connection to become available
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = this._waiting.indexOf(waiter);
        if (index > -1) {
          this._waiting.splice(index, 1);
        }
        reject(new Error('Connection pool exhausted'));
      }, this._config.poolTimeout);

      const waiter = { resolve, reject, timeout };
      this._waiting.push(waiter);
    });
  }

  /**
   * Return a connection to the pool.
   * @param {PooledConnection} conn
   */
  _return(conn) {
    if (conn.valid && !this._closed) {
      // Check if anyone is waiting
      if (this._waiting.length > 0) {
        const waiter = this._waiting.shift();
        clearTimeout(waiter.timeout);
        waiter.resolve(conn);
      } else {
        this._pool.push(conn);
      }
    } else {
      conn.close();
      // Replace with new connection
      if (!this._closed) {
        this._createConnection()
          .then((newConn) => {
            if (this._waiting.length > 0) {
              const waiter = this._waiting.shift();
              clearTimeout(waiter.timeout);
              waiter.resolve(newConn);
            } else {
              this._pool.push(newConn);
            }
          })
          .catch(() => {
            // Ignore creation errors
          });
      }
    }
  }

  /**
   * Send a request using a pooled connection.
   * @param {Object} request
   * @returns {Promise<Object>}
   */
  async sendRequest(request) {
    const conn = await this._borrow();

    try {
      const response = await this._sendOnConnection(conn, request);
      this._return(conn);
      return response;
    } catch (err) {
      conn.invalidate();
      this._return(conn);
      throw err;
    }
  }

  /**
   * Send a request on a specific connection.
   * @private
   */
  async _sendOnConnection(conn, request) {
    const socket = conn.socket;

    // Add auth
    if (this._sessionId) {
      request.auth = this._sessionId;
    } else if (this._config.token) {
      request.auth = this._config.token;
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
      }, this._config.readTimeout);

      let responseBuffer = Buffer.alloc(0);
      let expectedLength = null;
      let headerResponse = null;
      let stage = 'header';
      const ipcChunks = [];

      const cleanup = () => {
        clearTimeout(timeout);
        socket.removeListener('data', onData);
        socket.removeListener('error', onError);
      };

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

          const frameData = responseBuffer.slice(0, expectedLength);
          responseBuffer = responseBuffer.slice(expectedLength);
          expectedLength = null;

          if (stage === 'header') {
            try {
              const response = JSON.parse(frameData.toString('utf8'));
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
              cleanup();
              resolve(response);
              return;
            } catch (err) {
              cleanup();
              reject(new Error(`Failed to parse response: ${err.message}`));
              return;
            }
          } else if (stage === 'ipc') {
            if (frameData.length !== headerResponse.ipc_len) {
              cleanup();
              reject(new Error(`IPC length mismatch`));
              return;
            }
            headerResponse.ipc_bytes = frameData;
            cleanup();
            resolve(headerResponse);
            return;
          } else if (stage === 'ipc_stream') {
            if (frameData.length === 0) {
              headerResponse.ipc_bytes = Buffer.concat(ipcChunks);
              cleanup();
              resolve(headerResponse);
              return;
            }
            ipcChunks.push(frameData);
          }
        }
      };

      const onError = (err) => {
        cleanup();
        reject(err);
      };

      socket.on('data', onData);
      socket.on('error', onError);
      socket.write(frame);
    });
  }

  /**
   * Check server health.
   * @returns {Promise<void>}
   */
  async health() {
    const response = await this.sendRequest({ op: 'health' });
    if (response.status !== 'ok') {
      throw new Error(response.message || 'Health check failed');
    }
  }

  /**
   * Login with username and password.
   * @param {string} username
   * @param {string} password
   * @returns {Promise<void>}
   */
  async login(username, password) {
    const response = await this.sendRequest({
      op: 'login',
      username,
      password,
    });
    if (response.status !== 'ok') {
      throw new Error(response.message || 'Login failed');
    }
    this._sessionId = response.session_id;
  }

  /**
   * Logout from server.
   * @returns {Promise<void>}
   */
  async logout() {
    const response = await this.sendRequest({ op: 'logout' });
    if (response.status !== 'ok') {
      throw new Error(response.message || 'Logout failed');
    }
    this._sessionId = null;
  }

  /**
   * Close the pool.
   */
  close() {
    this._closed = true;
    for (const conn of this._pool) {
      conn.close();
    }
    this._pool = [];

    for (const waiter of this._waiting) {
      clearTimeout(waiter.timeout);
      waiter.reject(new Error('Pool closed'));
    }
    this._waiting = [];
  }
}

/**
 * High-performance pooled BoyoDB client.
 */
class PooledClient {
  /**
   * @param {PoolConfig} config
   */
  constructor(config) {
    this._config = config;
    this._pool = new ConnectionPool(config);
    this._initialized = false;
  }

  /**
   * Initialize the client and pool.
   * @returns {Promise<void>}
   */
  async connect() {
    if (this._initialized) return;
    await this._pool.initialize();
    await this._pool.health();
    this._initialized = true;
  }

  /**
   * Execute a SQL query.
   * @param {string} sql
   * @param {Object} [options={}]
   * @returns {Promise<Object>}
   */
  async query(sql, options = {}) {
    if (!this._initialized) {
      await this.connect();
    }

    const database = options.database || this._config.database;
    const timeout = options.timeout || this._config.queryTimeout;

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

    const response = await this._pool.sendRequest(request);

    if (response.status !== 'ok') {
      throw new Error(response.message || 'Query failed');
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
   * @param {string} sql
   * @param {Object} [options={}]
   * @returns {Promise<void>}
   */
  async exec(sql, options = {}) {
    if (!this._initialized) {
      await this.connect();
    }

    const database = options.database || this._config.database;
    const timeout = options.timeout || this._config.queryTimeout;

    const request = {
      op: 'query',
      sql,
      timeout_millis: timeout,
    };
    if (database) {
      request.database = database;
    }

    const response = await this._pool.sendRequest(request);

    if (response.status !== 'ok') {
      throw new Error(response.message || 'Exec failed');
    }
  }

  /**
   * Login with credentials.
   * @param {string} username
   * @param {string} password
   * @returns {Promise<void>}
   */
  async login(username, password) {
    if (!this._initialized) {
      await this.connect();
    }
    await this._pool.login(username, password);
  }

  /**
   * Logout from server.
   * @returns {Promise<void>}
   */
  async logout() {
    await this._pool.logout();
  }

  /**
   * Close the client.
   * @returns {Promise<void>}
   */
  async close() {
    this._pool.close();
    this._initialized = false;
  }

  _isSelectLike(sql) {
    const trimmed = sql.trimStart().toLowerCase();
    return trimmed.startsWith('select ') || trimmed.startsWith('with ');
  }

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
}

module.exports = { PoolConfig, ConnectionPool, PooledClient };
