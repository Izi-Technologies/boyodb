/**
 * Transaction support for BoyoDB Node.js driver.
 *
 * Provides ACID transaction support with savepoints and isolation levels.
 */

/**
 * Isolation levels for transactions.
 * @enum {string}
 */
const IsolationLevel = {
  READ_UNCOMMITTED: 'READ UNCOMMITTED',
  READ_COMMITTED: 'READ COMMITTED',
  REPEATABLE_READ: 'REPEATABLE READ',
  SERIALIZABLE: 'SERIALIZABLE',
  SNAPSHOT: 'SNAPSHOT',
};

/**
 * Transaction wrapper for ACID operations.
 *
 * @example
 * const tx = client.transaction();
 * try {
 *   await tx.begin();
 *   await tx.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
 *   await tx.execute("UPDATE accounts SET balance = balance - 100");
 *   await tx.commit();
 * } catch (err) {
 *   await tx.rollback();
 *   throw err;
 * }
 *
 * // Or with auto-commit/rollback:
 * await client.withTransaction(async (tx) => {
 *   await tx.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
 *   await tx.execute("UPDATE accounts SET balance = balance - 100");
 * });
 */
class Transaction {
  /**
   * Create a new transaction.
   * @param {Client} client - BoyoDB client
   * @param {Object} [options={}] - Transaction options
   * @param {IsolationLevel} [options.isolationLevel] - Isolation level
   * @param {boolean} [options.readOnly=false] - Read-only transaction
   */
  constructor(client, options = {}) {
    this._client = client;
    this._isolationLevel = options.isolationLevel || null;
    this._readOnly = options.readOnly || false;
    this._active = false;
    this._savepoints = [];
  }

  /**
   * Start the transaction.
   * @returns {Promise<void>}
   */
  async begin() {
    let sql = 'BEGIN';
    if (this._isolationLevel) {
      sql += ` ISOLATION LEVEL ${this._isolationLevel}`;
    }
    if (this._readOnly) {
      sql += ' READ ONLY';
    }

    await this._client.exec(sql);
    this._active = true;
  }

  /**
   * Commit the transaction.
   * @returns {Promise<void>}
   */
  async commit() {
    if (this._active) {
      await this._client.exec('COMMIT');
      this._active = false;
      this._savepoints = [];
    }
  }

  /**
   * Rollback the transaction.
   * @returns {Promise<void>}
   */
  async rollback() {
    if (this._active) {
      await this._client.exec('ROLLBACK');
      this._active = false;
      this._savepoints = [];
    }
  }

  /**
   * Create a savepoint.
   * @param {string} name - Savepoint name
   * @returns {Promise<void>}
   */
  async savepoint(name) {
    if (!this._active) {
      throw new Error('No active transaction');
    }
    await this._client.exec(`SAVEPOINT ${name}`);
    this._savepoints.push(name);
  }

  /**
   * Rollback to a savepoint.
   * @param {string} name - Savepoint name
   * @returns {Promise<void>}
   */
  async rollbackTo(name) {
    if (!this._active) {
      throw new Error('No active transaction');
    }
    await this._client.exec(`ROLLBACK TO SAVEPOINT ${name}`);
    const idx = this._savepoints.indexOf(name);
    if (idx !== -1) {
      this._savepoints = this._savepoints.slice(0, idx + 1);
    }
  }

  /**
   * Release a savepoint.
   * @param {string} name - Savepoint name
   * @returns {Promise<void>}
   */
  async release(name) {
    if (!this._active) {
      throw new Error('No active transaction');
    }
    await this._client.exec(`RELEASE SAVEPOINT ${name}`);
    const idx = this._savepoints.indexOf(name);
    if (idx !== -1) {
      this._savepoints.splice(idx, 1);
    }
  }

  /**
   * Execute SQL within the transaction.
   * @param {string} sql - SQL statement
   * @param {string} [database] - Database to use
   * @returns {Promise<QueryResult>}
   */
  async execute(sql, database) {
    if (!this._active) {
      throw new Error('No active transaction');
    }
    return this._client.query(sql, database);
  }

  /**
   * Check if transaction is active.
   * @returns {boolean}
   */
  get isActive() {
    return this._active;
  }
}

module.exports = {
  IsolationLevel,
  Transaction,
};
