/**
 * External data source helpers for BoyoDB Node.js driver.
 *
 * Query S3, URLs, HDFS, and files without importing data.
 */

/**
 * S3 connection configuration.
 * @typedef {Object} S3Config
 * @property {string} [region] - AWS region
 * @property {string} [endpoint] - Custom endpoint
 * @property {string} [accessKey] - Access key ID
 * @property {string} [secretKey] - Secret access key
 * @property {string} [sessionToken] - Session token
 * @property {boolean} [pathStyle=false] - Use path-style addressing
 */

/**
 * External tables helper for querying external data sources.
 *
 * @example
 * const ext = new ExternalTables(client);
 *
 * // Query S3
 * const results = await ext.queryS3(
 *   's3://my-bucket/data/events.parquet',
 *   'SELECT event_type, COUNT(*) as cnt FROM data GROUP BY event_type'
 * );
 *
 * // Query URL
 * const csvData = await ext.queryUrl(
 *   'https://example.com/data.csv',
 *   'SELECT * FROM data WHERE value > 100'
 * );
 */
class ExternalTables {
  /**
   * Create external tables helper.
   * @param {Client} client - BoyoDB client
   */
  constructor(client) {
    this._client = client;
  }

  /**
   * Query data from S3.
   * @param {string} path - S3 path (s3://bucket/path/file)
   * @param {string} [sql='SELECT *'] - SQL query (use 'data' as table alias)
   * @param {Object} [options={}] - Options
   * @param {string} [options.format] - File format (parquet, csv, json)
   * @param {S3Config} [options.config] - S3 configuration
   * @param {number} [options.limit] - Row limit
   * @returns {Promise<Object[]>}
   */
  async queryS3(path, sql = 'SELECT *', options = {}) {
    const formatArg = options.format ? `, '${options.format}'` : '';
    const limitClause = options.limit ? ` LIMIT ${options.limit}` : '';

    let fullSql;
    if (sql.trim().toUpperCase() === 'SELECT *') {
      fullSql = `SELECT * FROM s3('${path}'${formatArg})${limitClause}`;
    } else {
      fullSql = `
        SELECT * FROM (
          ${sql.replace(/FROM\s+data/gi, `FROM s3('${path}'${formatArg})`)}
        ) AS subq
        ${limitClause}
      `;
    }

    return await this._client.query(fullSql);
  }

  /**
   * Query data from HTTP URL.
   * @param {string} url - HTTP(S) URL
   * @param {string} [sql='SELECT *'] - SQL query
   * @param {Object} [options={}] - Options
   * @param {string} [options.format] - File format
   * @param {Object} [options.headers] - HTTP headers
   * @param {number} [options.limit] - Row limit
   * @returns {Promise<Object[]>}
   */
  async queryUrl(url, sql = 'SELECT *', options = {}) {
    const formatArg = options.format ? `, '${options.format}'` : '';
    const limitClause = options.limit ? ` LIMIT ${options.limit}` : '';

    let fullSql;
    if (sql.trim().toUpperCase() === 'SELECT *') {
      fullSql = `SELECT * FROM url('${url}'${formatArg})${limitClause}`;
    } else {
      fullSql = `
        SELECT * FROM (
          ${sql.replace(/FROM\s+data/gi, `FROM url('${url}'${formatArg})`)}
        ) AS subq
        ${limitClause}
      `;
    }

    return await this._client.query(fullSql);
  }

  /**
   * Query data from local file.
   * @param {string} path - Local file path
   * @param {string} [sql='SELECT *'] - SQL query
   * @param {Object} [options={}] - Options
   * @param {string} [options.format] - File format
   * @param {number} [options.limit] - Row limit
   * @returns {Promise<Object[]>}
   */
  async queryFile(path, sql = 'SELECT *', options = {}) {
    const formatArg = options.format ? `, '${options.format}'` : '';
    const limitClause = options.limit ? ` LIMIT ${options.limit}` : '';

    let fullSql;
    if (sql.trim().toUpperCase() === 'SELECT *') {
      fullSql = `SELECT * FROM file('${path}'${formatArg})${limitClause}`;
    } else {
      fullSql = `
        SELECT * FROM (
          ${sql.replace(/FROM\s+data/gi, `FROM file('${path}'${formatArg})`)}
        ) AS subq
        ${limitClause}
      `;
    }

    return await this._client.query(fullSql);
  }

  /**
   * Query data from HDFS.
   * @param {string} path - HDFS path (hdfs://namenode/path)
   * @param {string} [sql='SELECT *'] - SQL query
   * @param {Object} [options={}] - Options
   * @param {string} [options.format] - File format
   * @param {number} [options.limit] - Row limit
   * @returns {Promise<Object[]>}
   */
  async queryHdfs(path, sql = 'SELECT *', options = {}) {
    const formatArg = options.format ? `, '${options.format}'` : '';
    const limitClause = options.limit ? ` LIMIT ${options.limit}` : '';

    let fullSql;
    if (sql.trim().toUpperCase() === 'SELECT *') {
      fullSql = `SELECT * FROM hdfs('${path}'${formatArg})${limitClause}`;
    } else {
      fullSql = `
        SELECT * FROM (
          ${sql.replace(/FROM\s+data/gi, `FROM hdfs('${path}'${formatArg})`)}
        ) AS subq
        ${limitClause}
      `;
    }

    return await this._client.query(fullSql);
  }

  /**
   * Create a named external table.
   * @param {string} name - Table name
   * @param {Object} config - Table configuration
   * @param {string} config.sourceType - Source type (s3, url, hdfs, file)
   * @param {string} config.location - Source location
   * @param {string} config.format - File format
   * @param {Object[]} config.columns - Column definitions [{name, type}]
   * @param {string[]} [config.partitionColumns] - Partition columns
   * @returns {Promise<void>}
   */
  async createExternalTable(name, config) {
    const colDefs = config.columns
      .map(c => `${c.name} ${c.type}`)
      .join(', ');

    let partitionClause = '';
    if (config.partitionColumns && config.partitionColumns.length > 0) {
      partitionClause = ` PARTITION BY (${config.partitionColumns.join(', ')})`;
    }

    const sql = `
      CREATE EXTERNAL TABLE ${name} (${colDefs})
      STORED AS ${config.format.toUpperCase()}
      LOCATION '${config.sourceType}://${config.location.replace(new RegExp(`^${config.sourceType}://`), '')}'
      ${partitionClause}
    `;

    await this._client.exec(sql);
  }

  /**
   * Drop an external table.
   * @param {string} name - Table name
   * @returns {Promise<void>}
   */
  async dropExternalTable(name) {
    await this._client.exec(`DROP EXTERNAL TABLE IF EXISTS ${name}`);
  }
}

/**
 * Vector similarity search helper.
 *
 * @example
 * const vec = new VectorSearch(client);
 *
 * // Create index
 * await vec.createIndex('documents', 'embedding', {
 *   distanceMetric: 'cosine',
 *   m: 16,
 *   efConstruction: 200
 * });
 *
 * // Search
 * const results = await vec.search('documents', 'embedding', queryVector, {
 *   k: 10,
 *   where: "category = 'tech'"
 * });
 */
class VectorSearch {
  /**
   * Create vector search helper.
   * @param {Client} client - BoyoDB client
   */
  constructor(client) {
    this._client = client;
  }

  /**
   * Create a vector index.
   * @param {string} table - Table name
   * @param {string} column - Vector column
   * @param {Object} [options={}] - Index options
   * @param {string} [options.indexName] - Custom index name
   * @param {string} [options.indexType='hnsw'] - Index type (hnsw, ivfflat)
   * @param {string} [options.distanceMetric='cosine'] - Distance metric (cosine, l2, ip)
   * @param {number} [options.m=16] - HNSW M parameter
   * @param {number} [options.efConstruction=200] - HNSW ef_construction
   * @returns {Promise<void>}
   */
  async createIndex(table, column, options = {}) {
    const name = options.indexName || `idx_${table.replace(/\./g, '_')}_${column}_vec`;
    const indexType = options.indexType || 'hnsw';
    const distanceMetric = options.distanceMetric || 'cosine';
    const m = options.m || 16;
    const efConstruction = options.efConstruction || 200;

    const sql = `
      CREATE INDEX ${name} ON ${table}
      USING ${indexType} (${column} ${distanceMetric})
      WITH (m = ${m}, ef_construction = ${efConstruction})
    `;

    await this._client.exec(sql);
  }

  /**
   * Search for similar vectors.
   * @param {string} table - Table name
   * @param {string} column - Vector column
   * @param {number[]} queryVector - Query vector
   * @param {Object} [options={}] - Search options
   * @param {number} [options.k=10] - Number of results
   * @param {string} [options.where] - Filter condition
   * @param {number} [options.efSearch=100] - HNSW ef_search
   * @param {string[]} [options.returnColumns] - Columns to return
   * @returns {Promise<Object[]>}
   */
  async search(table, column, queryVector, options = {}) {
    const k = options.k || 10;
    const vecStr = `[${queryVector.join(',')}]`;
    const cols = options.returnColumns ? options.returnColumns.join(', ') : '*';
    const whereClause = options.where ? `WHERE ${options.where}` : '';

    const sql = `
      SELECT ${cols}, ${column} <=> '${vecStr}'::vector AS distance
      FROM ${table}
      ${whereClause}
      ORDER BY ${column} <=> '${vecStr}'::vector
      LIMIT ${k}
    `;

    return await this._client.query(sql);
  }

  /**
   * Hybrid vector + full-text search.
   * @param {string} table - Table name
   * @param {string} vectorColumn - Vector column
   * @param {string} textColumn - Text column for FTS
   * @param {number[]} queryVector - Query vector
   * @param {string} queryText - Query text
   * @param {Object} [options={}] - Search options
   * @param {number} [options.k=10] - Number of results
   * @param {number} [options.vectorWeight=0.7] - Vector similarity weight
   * @param {number} [options.textWeight=0.3] - Text relevance weight
   * @returns {Promise<Object[]>}
   */
  async hybridSearch(table, vectorColumn, textColumn, queryVector, queryText, options = {}) {
    const k = options.k || 10;
    const vectorWeight = options.vectorWeight || 0.7;
    const textWeight = options.textWeight || 0.3;
    const vecStr = `[${queryVector.join(',')}]`;
    const escapedText = queryText.replace(/'/g, "''");

    const sql = `
      SELECT *,
        (${vectorWeight} * (1.0 - (${vectorColumn} <=> '${vecStr}'::vector))) +
        (${textWeight} * ts_rank(${textColumn}_tsvector, plainto_tsquery('${escapedText}')))
        AS hybrid_score
      FROM ${table}
      WHERE ${textColumn}_tsvector @@ plainto_tsquery('${escapedText}')
         OR ${vectorColumn} <=> '${vecStr}'::vector < 0.5
      ORDER BY hybrid_score DESC
      LIMIT ${k}
    `;

    return await this._client.query(sql);
  }

  /**
   * Drop a vector index.
   * @param {string} indexName - Index name
   * @returns {Promise<void>}
   */
  async dropIndex(indexName) {
    await this._client.exec(`DROP INDEX IF EXISTS ${indexName}`);
  }
}

module.exports = {
  ExternalTables,
  VectorSearch,
};
