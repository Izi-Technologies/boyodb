/**
 * Pub/Sub and CDC subscription support for BoyoDB Node.js driver.
 *
 * Provides real-time messaging and change data capture subscriptions.
 */

/**
 * Change types for CDC events.
 * @enum {string}
 */
const ChangeType = {
  INSERT: 'INSERT',
  UPDATE: 'UPDATE',
  DELETE: 'DELETE',
};

/**
 * Message received from a subscription.
 * @typedef {Object} Message
 * @property {string} channel - Channel name
 * @property {*} payload - Message payload
 * @property {Date} timestamp - Message timestamp
 * @property {string} [id] - Message ID
 */

/**
 * CDC change event.
 * @typedef {Object} ChangeEvent
 * @property {string} table - Table name
 * @property {ChangeType} changeType - Type of change
 * @property {Object} [before] - Row before change (for UPDATE/DELETE)
 * @property {Object} after - Row after change (for INSERT/UPDATE)
 * @property {number} lsn - Log sequence number
 * @property {Date} timestamp - Change timestamp
 */

/**
 * Pub/Sub subscriber for real-time messaging.
 *
 * @example
 * const subscriber = new Subscriber(client, ['events', 'alerts'], (msg) => {
 *   console.log(`[${msg.channel}] ${JSON.stringify(msg.payload)}`);
 * });
 *
 * subscriber.start();
 *
 * // Later...
 * subscriber.stop();
 */
class Subscriber {
  /**
   * Create a new subscriber.
   * @param {Client} client - BoyoDB client
   * @param {string[]} channels - Channels to subscribe to
   * @param {Function} onMessage - Callback for received messages
   * @param {Object} [options={}] - Subscriber options
   * @param {Function} [options.onError] - Error callback
   * @param {number} [options.reconnectDelayMs=1000] - Reconnect delay
   */
  constructor(client, channels, onMessage, options = {}) {
    this._client = client;
    this._channels = channels;
    this._onMessage = onMessage;
    this._onError = options.onError || console.error;
    this._reconnectDelayMs = options.reconnectDelayMs || 1000;

    this._running = false;
    this._pollTimer = null;
  }

  /**
   * Start the subscriber.
   */
  start() {
    if (this._running) return;
    this._running = true;

    // Subscribe to channels
    for (const channel of this._channels) {
      this._subscribe(channel).catch(this._onError);
    }

    this._startPolling();
  }

  /**
   * Stop the subscriber.
   */
  stop() {
    this._running = false;
    if (this._pollTimer) {
      clearTimeout(this._pollTimer);
      this._pollTimer = null;
    }

    // Unsubscribe from channels
    for (const channel of this._channels) {
      this._unsubscribe(channel).catch(this._onError);
    }
  }

  /**
   * Add a channel to subscribe to.
   * @param {string} channel - Channel name
   */
  async addChannel(channel) {
    if (!this._channels.includes(channel)) {
      this._channels.push(channel);
      if (this._running) {
        await this._subscribe(channel);
      }
    }
  }

  /**
   * Remove a channel subscription.
   * @param {string} channel - Channel name
   */
  async removeChannel(channel) {
    const idx = this._channels.indexOf(channel);
    if (idx !== -1) {
      this._channels.splice(idx, 1);
      if (this._running) {
        await this._unsubscribe(channel);
      }
    }
  }

  async _subscribe(channel) {
    await this._client.exec(`SUBSCRIBE '${channel}'`);
  }

  async _unsubscribe(channel) {
    await this._client.exec(`UNSUBSCRIBE '${channel}'`);
  }

  _startPolling() {
    if (!this._running) return;

    this._pollTimer = setTimeout(async () => {
      try {
        const result = await this._client.query('POLL MESSAGES');
        for (const row of result) {
          const msg = {
            channel: row.channel,
            payload: typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload,
            timestamp: new Date(row.timestamp),
            id: row.id,
          };
          this._onMessage(msg);
        }
      } catch (err) {
        this._onError(err);
      }
      this._startPolling();
    }, 100);
  }
}

/**
 * CDC subscriber for change data capture.
 *
 * @example
 * const cdc = new CDCSubscriber(client, 'mydb.users', (event) => {
 *   console.log(`${event.changeType}: ${JSON.stringify(event.after)}`);
 * }, {
 *   includeBefore: true,
 *   changeTypes: [ChangeType.INSERT, ChangeType.UPDATE]
 * });
 *
 * cdc.start();
 *
 * // Later...
 * cdc.stop();
 */
class CDCSubscriber {
  /**
   * Create a new CDC subscriber.
   * @param {Client} client - BoyoDB client
   * @param {string} table - Table to watch (database.table)
   * @param {Function} onChange - Callback for change events
   * @param {Object} [options={}] - Subscriber options
   * @param {boolean} [options.includeBefore=false] - Include before image
   * @param {ChangeType[]} [options.changeTypes] - Filter by change types
   * @param {Function} [options.onError] - Error callback
   * @param {number} [options.startLsn] - Starting LSN
   */
  constructor(client, table, onChange, options = {}) {
    this._client = client;
    this._table = table;
    this._onChange = onChange;
    this._includeBefore = options.includeBefore || false;
    this._changeTypes = options.changeTypes || Object.values(ChangeType);
    this._onError = options.onError || console.error;
    this._startLsn = options.startLsn || 0;

    this._running = false;
    this._currentLsn = this._startLsn;
    this._pollTimer = null;
  }

  /**
   * Start the CDC subscriber.
   */
  start() {
    if (this._running) return;
    this._running = true;

    this._startCDC().catch(this._onError);
    this._startPolling();
  }

  /**
   * Stop the CDC subscriber.
   */
  stop() {
    this._running = false;
    if (this._pollTimer) {
      clearTimeout(this._pollTimer);
      this._pollTimer = null;
    }
    this._stopCDC().catch(this._onError);
  }

  /**
   * Get current LSN position.
   * @returns {number}
   */
  get currentLsn() {
    return this._currentLsn;
  }

  async _startCDC() {
    const opts = [];
    if (this._includeBefore) {
      opts.push("include_before = true");
    }
    if (this._startLsn > 0) {
      opts.push(`start_lsn = ${this._startLsn}`);
    }

    const optStr = opts.length > 0 ? ` WITH (${opts.join(', ')})` : '';
    await this._client.exec(`START CDC ON ${this._table}${optStr}`);
  }

  async _stopCDC() {
    await this._client.exec(`STOP CDC ON ${this._table}`);
  }

  _startPolling() {
    if (!this._running) return;

    this._pollTimer = setTimeout(async () => {
      try {
        const result = await this._client.query(
          `POLL CDC ${this._table} FROM ${this._currentLsn}`
        );

        for (const row of result) {
          const changeType = row.change_type || row.changeType;

          // Filter by change type
          if (!this._changeTypes.includes(changeType)) {
            continue;
          }

          const event = {
            table: this._table,
            changeType: changeType,
            before: row.before,
            after: row.after,
            lsn: row.lsn,
            timestamp: new Date(row.timestamp),
          };

          this._onChange(event);
          this._currentLsn = Math.max(this._currentLsn, row.lsn + 1);
        }
      } catch (err) {
        this._onError(err);
      }
      this._startPolling();
    }, 100);
  }
}

/**
 * Publisher for pub/sub messaging.
 *
 * @example
 * const publisher = new Publisher(client);
 * await publisher.publish('events', { type: 'user_signup', userId: 123 });
 * await publisher.publish('alerts', { level: 'warning', message: 'High CPU' });
 */
class Publisher {
  /**
   * Create a new publisher.
   * @param {Client} client - BoyoDB client
   */
  constructor(client) {
    this._client = client;
  }

  /**
   * Publish a message to a channel.
   * @param {string} channel - Channel name
   * @param {*} payload - Message payload
   * @returns {Promise<void>}
   */
  async publish(channel, payload) {
    const payloadStr = typeof payload === 'string'
      ? payload
      : JSON.stringify(payload);

    const escaped = payloadStr.replace(/'/g, "''");
    await this._client.exec(`PUBLISH '${channel}' '${escaped}'`);
  }

  /**
   * Publish multiple messages to a channel.
   * @param {string} channel - Channel name
   * @param {*[]} payloads - Array of payloads
   * @returns {Promise<void>}
   */
  async publishMany(channel, payloads) {
    for (const payload of payloads) {
      await this.publish(channel, payload);
    }
  }
}

module.exports = {
  ChangeType,
  Subscriber,
  CDCSubscriber,
  Publisher,
};
