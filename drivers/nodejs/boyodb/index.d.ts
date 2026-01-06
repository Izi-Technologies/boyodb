/**
 * boyodb Node.js Driver TypeScript definitions
 */

export interface ClientConfig {
  /** Enable TLS encryption */
  tls?: boolean;
  /** Path to CA certificate file */
  caFile?: string;
  /** Skip TLS verification (insecure) */
  insecureSkipVerify?: boolean;
  /** Connection timeout in milliseconds */
  connectTimeout?: number;
  /** Read timeout in milliseconds */
  readTimeout?: number;
  /** Write timeout in milliseconds */
  writeTimeout?: number;
  /** Authentication token */
  token?: string;
  /** Maximum connection retry attempts */
  maxRetries?: number;
  /** Delay between retries in milliseconds */
  retryDelay?: number;
  /** Default database for queries */
  database?: string;
  /** Default query timeout in milliseconds */
  queryTimeout?: number;
}

export interface QueryResult {
  /** Array of row objects */
  rows: Record<string, any>[];
  /** Column names */
  columns: string[];
  /** Total number of rows */
  rowCount: number;
  /** Number of segments scanned */
  segmentsScanned: number;
  /** Bytes skipped due to pruning */
  dataSkippedBytes: number;
}

export interface TableInfo {
  database: string;
  name: string;
  schema_json?: string;
}

export interface QueryOptions {
  /** Database to use */
  database?: string;
  /** Query timeout in milliseconds */
  timeout?: number;
}

export interface PrepareOptions {
  /** Database to use */
  database?: string;
}

export interface ExecutePreparedOptions {
  /** Query timeout in milliseconds */
  timeout?: number;
}

export interface IngestCSVOptions {
  /** CSV has header row */
  hasHeader?: boolean;
  /** Field delimiter */
  delimiter?: string;
}

export class Client {
  /**
   * Create a new boyodb client.
   * @param host - Server address in host:port format
   * @param config - Configuration options
   */
  constructor(host: string, config?: ClientConfig);

  /**
   * Connect to the server.
   */
  connect(): Promise<void>;

  /**
   * Close the connection.
   */
  close(): Promise<void>;

  /**
   * Check server health.
   */
  health(): Promise<void>;

  /**
   * Login with username and password.
   * @param username - Username
   * @param password - Password
   */
  login(username: string, password: string): Promise<void>;

  /**
   * Logout from the server.
   */
  logout(): Promise<void>;

  /**
   * Execute a SQL query.
   * @param sql - SQL query string
   * @param options - Query options
   */
  query(sql: string, options?: QueryOptions): Promise<QueryResult>;

  /**
   * Prepare a SELECT query on the server.
   * @param sql - SQL query string
   * @param options - Prepare options
   */
  prepare(sql: string, options?: PrepareOptions): Promise<string>;

  /**
   * Execute a prepared statement using binary IPC responses.
   * @param preparedId - Prepared statement id
   * @param options - Execution options
   */
  executePreparedBinary(preparedId: string, options?: ExecutePreparedOptions): Promise<QueryResult>;

  /**
   * Execute a SQL statement that doesn't return rows.
   * @param sql - SQL statement
   * @param options - Execution options
   */
  exec(sql: string, options?: QueryOptions): Promise<QueryResult>;

  /**
   * Create a new database.
   * @param name - Database name
   */
  createDatabase(name: string): Promise<void>;

  /**
   * Create a new table.
   * @param database - Database name
   * @param table - Table name
   */
  createTable(database: string, table: string): Promise<void>;

  /**
   * List all databases.
   */
  listDatabases(): Promise<string[]>;

  /**
   * List tables, optionally filtered by database.
   * @param database - Database to filter by
   */
  listTables(database?: string): Promise<TableInfo[]>;

  /**
   * Get query execution plan.
   * @param sql - SQL query
   */
  explain(sql: string): Promise<object>;

  /**
   * Get server metrics.
   */
  metrics(): Promise<object>;

  /**
   * Ingest CSV data into a table.
   * @param database - Database name
   * @param table - Table name
   * @param csvData - CSV data
   * @param options - Ingest options
   */
  ingestCSV(
    database: string,
    table: string,
    csvData: Buffer | string,
    options?: IngestCSVOptions
  ): Promise<void>;

  /**
   * Ingest Arrow IPC data into a table.
   * @param database - Database name
   * @param table - Table name
   * @param ipcData - Arrow IPC data
   */
  ingestIPC(database: string, table: string, ipcData: Buffer): Promise<void>;

  /**
   * Set the default database.
   * @param database - Database name
   */
  setDatabase(database: string): void;

  /**
   * Set the authentication token.
   * @param token - Auth token
   */
  setToken(token: string): void;
}
