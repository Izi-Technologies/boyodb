/**
 * Analytics helpers for BoyoDB Node.js driver.
 *
 * Provides approximate aggregations, time series, and graph analytics.
 */

/**
 * Cardinality estimate from HyperLogLog.
 * @typedef {Object} CardinalityEstimate
 * @property {number} estimate - Estimated count
 * @property {number} relativeError - Relative error rate
 */

/**
 * Quantile estimate from T-Digest.
 * @typedef {Object} QuantileEstimate
 * @property {number} quantile - Percentile (0-100)
 * @property {number} value - Estimated value
 */

/**
 * Frequency estimate from Count-Min Sketch.
 * @typedef {Object} FrequencyEstimate
 * @property {string} item - Item value
 * @property {number} count - Estimated count
 * @property {boolean} isOverestimate - Whether count may be overestimated
 */

/**
 * Approximate aggregation functions using probabilistic data structures.
 *
 * @example
 * const approx = new ApproximateFunctions(client);
 * const estimate = await approx.countDistinct('events', 'user_id');
 * console.log(`Unique users: ~${estimate.estimate} (±${estimate.relativeError * 100}%)`);
 */
class ApproximateFunctions {
  /**
   * Create approximate functions helper.
   * @param {Client} client - BoyoDB client
   */
  constructor(client) {
    this._client = client;
  }

  /**
   * Approximate distinct count using HyperLogLog.
   * @param {string} table - Table name
   * @param {string} column - Column to count
   * @param {Object} [options={}] - Options
   * @param {string} [options.where] - WHERE clause
   * @param {number} [options.precision=14] - HLL precision (10-18)
   * @returns {Promise<CardinalityEstimate>}
   */
  async countDistinct(table, column, options = {}) {
    const where = options.where ? `WHERE ${options.where}` : '';
    const precision = options.precision || 14;

    const sql = `
      SELECT approx_count_distinct(${column}, ${precision}) as estimate
      FROM ${table}
      ${where}
    `;

    const result = await this._client.query(sql);
    const estimate = result.length > 0 ? result[0].estimate : 0;

    // Error rate: 1.04 / sqrt(2^precision)
    const errorRate = 1.04 / Math.pow(2, precision / 2);

    return {
      estimate: Math.round(estimate),
      relativeError: errorRate,
    };
  }

  /**
   * Approximate percentiles using T-Digest.
   * @param {string} table - Table name
   * @param {string} column - Numeric column
   * @param {number|number[]} percentiles - Percentile(s) 0-100
   * @param {Object} [options={}] - Options
   * @param {string} [options.where] - WHERE clause
   * @param {number} [options.compression=100] - T-Digest compression
   * @returns {Promise<QuantileEstimate[]>}
   */
  async percentile(table, column, percentiles, options = {}) {
    const where = options.where ? `WHERE ${options.where}` : '';
    const compression = options.compression || 100;

    const pcts = Array.isArray(percentiles) ? percentiles : [percentiles];
    const results = [];

    for (const p of pcts) {
      const sql = `
        SELECT approx_percentile(${column}, ${p / 100.0}, ${compression}) as value
        FROM ${table}
        ${where}
      `;

      const result = await this._client.query(sql);
      const value = result.length > 0 ? result[0].value : null;

      results.push({
        quantile: p,
        value: value,
      });
    }

    return results;
  }

  /**
   * Approximate median (P50).
   * @param {string} table - Table name
   * @param {string} column - Numeric column
   * @param {string} [where] - WHERE clause
   * @returns {Promise<number|null>}
   */
  async median(table, column, where) {
    const results = await this.percentile(table, column, 50, { where });
    return results.length > 0 ? results[0].value : null;
  }

  /**
   * Find top-k most frequent values.
   * @param {string} table - Table name
   * @param {string} column - Column to analyze
   * @param {number} [k=10] - Number of top items
   * @param {string} [where] - WHERE clause
   * @returns {Promise<FrequencyEstimate[]>}
   */
  async topK(table, column, k = 10, where) {
    const whereClause = where ? `WHERE ${where}` : '';

    const sql = `
      SELECT ${column} as item, COUNT(*) as cnt
      FROM ${table}
      ${whereClause}
      GROUP BY ${column}
      ORDER BY cnt DESC
      LIMIT ${k}
    `;

    const result = await this._client.query(sql);

    return result.map(row => ({
      item: String(row.item),
      count: row.cnt,
      isOverestimate: false,
    }));
  }
}

/**
 * Time series analysis functions.
 *
 * @example
 * const ts = new TimeSeriesAnalytics(client);
 * const hourly = await ts.aggregateByTime('metrics', 'timestamp', 'value', '1 hour', 'avg');
 */
class TimeSeriesAnalytics {
  /**
   * Create time series analytics helper.
   * @param {Client} client - BoyoDB client
   */
  constructor(client) {
    this._client = client;
  }

  /**
   * Aggregate values by time buckets.
   * @param {string} table - Table name
   * @param {string} timeColumn - Timestamp column
   * @param {string} valueColumn - Value column
   * @param {string} bucket - Bucket size ('1 minute', '1 hour', '1 day')
   * @param {string} [aggregation='avg'] - Aggregation (avg, sum, min, max, count)
   * @param {string} [where] - WHERE clause
   * @returns {Promise<Object[]>}
   */
  async aggregateByTime(table, timeColumn, valueColumn, bucket, aggregation = 'avg', where) {
    const whereClause = where ? `WHERE ${where}` : '';
    const aggFunc = aggregation.toUpperCase();

    const sql = `
      SELECT
        time_bucket('${bucket}', ${timeColumn}) as bucket,
        ${aggFunc}(${valueColumn}) as value
      FROM ${table}
      ${whereClause}
      GROUP BY bucket
      ORDER BY bucket
    `;

    return await this._client.query(sql);
  }

  /**
   * Calculate moving average.
   * @param {string} table - Table name
   * @param {string} timeColumn - Timestamp column
   * @param {string} valueColumn - Value column
   * @param {number} windowSize - Window size in rows
   * @param {string} [where] - WHERE clause
   * @returns {Promise<Object[]>}
   */
  async movingAverage(table, timeColumn, valueColumn, windowSize, where) {
    const whereClause = where ? `WHERE ${where}` : '';

    const sql = `
      SELECT
        ${timeColumn} as timestamp,
        ${valueColumn} as value,
        AVG(${valueColumn}) OVER (
          ORDER BY ${timeColumn}
          ROWS BETWEEN ${windowSize - 1} PRECEDING AND CURRENT ROW
        ) as moving_avg
      FROM ${table}
      ${whereClause}
      ORDER BY ${timeColumn}
    `;

    return await this._client.query(sql);
  }

  /**
   * Detect anomalies using z-score.
   * @param {string} table - Table name
   * @param {string} timeColumn - Timestamp column
   * @param {string} valueColumn - Value column
   * @param {number} [thresholdStddev=3.0] - Standard deviations threshold
   * @param {string} [where] - WHERE clause
   * @returns {Promise<Object[]>}
   */
  async detectAnomalies(table, timeColumn, valueColumn, thresholdStddev = 3.0, where) {
    const whereClause = where ? `WHERE ${where}` : '';

    const sql = `
      WITH stats AS (
        SELECT AVG(${valueColumn}) as mean, STDDEV(${valueColumn}) as stddev
        FROM ${table} ${whereClause}
      )
      SELECT
        t.${timeColumn} as timestamp,
        t.${valueColumn} as value,
        (t.${valueColumn} - stats.mean) / NULLIF(stats.stddev, 0) as z_score
      FROM ${table} t, stats
      ${whereClause}
      HAVING ABS(z_score) > ${thresholdStddev}
      ORDER BY ABS(z_score) DESC
    `;

    return await this._client.query(sql);
  }

  /**
   * Calculate rate of change.
   * @param {string} table - Table name
   * @param {string} timeColumn - Timestamp column
   * @param {string} valueColumn - Value column
   * @param {string} [where] - WHERE clause
   * @returns {Promise<Object[]>}
   */
  async rateOfChange(table, timeColumn, valueColumn, where) {
    const whereClause = where ? `WHERE ${where}` : '';

    const sql = `
      SELECT
        ${timeColumn} as timestamp,
        ${valueColumn} as value,
        ${valueColumn} - LAG(${valueColumn}) OVER (ORDER BY ${timeColumn}) as delta,
        EXTRACT(EPOCH FROM ${timeColumn} - LAG(${timeColumn}) OVER (ORDER BY ${timeColumn})) as time_delta_sec
      FROM ${table}
      ${whereClause}
      ORDER BY ${timeColumn}
    `;

    return await this._client.query(sql);
  }
}

/**
 * Graph analytics functions.
 *
 * @example
 * const graph = new GraphAnalytics(client);
 * const path = await graph.shortestPath('edges', 'from_node', 'to_node', 'weight', 1, 10);
 */
class GraphAnalytics {
  /**
   * Create graph analytics helper.
   * @param {Client} client - BoyoDB client
   */
  constructor(client) {
    this._client = client;
  }

  /**
   * Find shortest path between two nodes.
   * @param {string} edgesTable - Table containing edges
   * @param {string} sourceColumn - Source node column
   * @param {string} targetColumn - Target node column
   * @param {string|null} weightColumn - Weight column (null for unweighted)
   * @param {*} startNode - Starting node ID
   * @param {*} endNode - Ending node ID
   * @returns {Promise<Array|null>}
   */
  async shortestPath(edgesTable, sourceColumn, targetColumn, weightColumn, startNode, endNode) {
    const weight = weightColumn || '1';

    const sql = `
      SELECT graph_shortest_path(
        '${edgesTable}',
        '${sourceColumn}',
        '${targetColumn}',
        '${weight}',
        ${startNode},
        ${endNode}
      ) as path
    `;

    const result = await this._client.query(sql);
    if (result.length > 0 && result[0].path) {
      return result[0].path;
    }
    return null;
  }

  /**
   * Calculate PageRank scores.
   * @param {string} edgesTable - Table containing edges
   * @param {string} sourceColumn - Source node column
   * @param {string} targetColumn - Target node column
   * @param {Object} [options={}] - Options
   * @param {number} [options.damping=0.85] - Damping factor
   * @param {number} [options.iterations=20] - Number of iterations
   * @param {number} [options.topK=10] - Return top-k nodes
   * @returns {Promise<Object[]>}
   */
  async pagerank(edgesTable, sourceColumn, targetColumn, options = {}) {
    const damping = options.damping || 0.85;
    const iterations = options.iterations || 20;
    const topK = options.topK || 10;

    const sql = `
      SELECT node, score
      FROM graph_pagerank(
        '${edgesTable}',
        '${sourceColumn}',
        '${targetColumn}',
        ${damping},
        ${iterations}
      )
      ORDER BY score DESC
      LIMIT ${topK}
    `;

    return await this._client.query(sql);
  }

  /**
   * Find connected components.
   * @param {string} edgesTable - Table containing edges
   * @param {string} sourceColumn - Source node column
   * @param {string} targetColumn - Target node column
   * @returns {Promise<Object[]>}
   */
  async connectedComponents(edgesTable, sourceColumn, targetColumn) {
    const sql = `
      SELECT node, component_id
      FROM graph_connected_components(
        '${edgesTable}',
        '${sourceColumn}',
        '${targetColumn}'
      )
      ORDER BY component_id, node
    `;

    return await this._client.query(sql);
  }

  /**
   * Calculate node degrees.
   * @param {string} edgesTable - Table containing edges
   * @param {string} sourceColumn - Source node column
   * @param {string} targetColumn - Target node column
   * @returns {Promise<Object[]>}
   */
  async degrees(edgesTable, sourceColumn, targetColumn) {
    const sql = `
      SELECT node,
        in_degree,
        out_degree,
        in_degree + out_degree as total_degree
      FROM (
        SELECT
          node,
          SUM(CASE WHEN is_target THEN 1 ELSE 0 END) as in_degree,
          SUM(CASE WHEN is_source THEN 1 ELSE 0 END) as out_degree
        FROM (
          SELECT ${sourceColumn} as node, TRUE as is_source, FALSE as is_target FROM ${edgesTable}
          UNION ALL
          SELECT ${targetColumn} as node, FALSE as is_source, TRUE as is_target FROM ${edgesTable}
        ) t
        GROUP BY node
      ) degrees
      ORDER BY total_degree DESC
    `;

    return await this._client.query(sql);
  }
}

module.exports = {
  ApproximateFunctions,
  TimeSeriesAnalytics,
  GraphAnalytics,
};
