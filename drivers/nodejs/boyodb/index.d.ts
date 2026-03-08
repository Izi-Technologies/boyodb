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

export interface TransactionOptions {
  /** Isolation level (READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE) */
  isolationLevel?: string;
  /** Start a read-only transaction */
  readOnly?: boolean;
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

  // Transaction Support

  /**
   * Start a new transaction.
   * @param options - Transaction options
   */
  begin(options?: TransactionOptions): Promise<void>;

  /**
   * Commit the current transaction.
   */
  commit(): Promise<void>;

  /**
   * Rollback the current transaction or to a savepoint.
   * @param savepoint - If provided, rollback to this savepoint
   */
  rollback(savepoint?: string): Promise<void>;

  /**
   * Create a savepoint.
   * @param name - Savepoint name
   */
  savepoint(name: string): Promise<void>;

  /**
   * Release a savepoint.
   * @param name - Savepoint name
   */
  releaseSavepoint(name: string): Promise<void>;

  /**
   * Execute a function within a transaction.
   * @param fn - Async function to execute
   * @param options - Transaction options
   */
  inTransaction<T>(fn: () => Promise<T>, options?: TransactionOptions): Promise<T>;

  // Vector Search Support

  /**
   * Perform a vector similarity search.
   * @param table - Table name (format: database.table or just table if database is set)
   * @param column - Vector column name
   * @param queryVector - Query vector
   * @param options - Search options
   */
  vectorSearch(
    table: string,
    column: string,
    queryVector: number[],
    options?: VectorSearchOptions
  ): Promise<VectorSearchResult[]>;

  /**
   * Perform a hybrid search combining vector similarity and text search.
   * @param table - Table name
   * @param vectorColumn - Vector column name
   * @param queryVector - Query vector
   * @param textColumn - Text column for text search
   * @param textQuery - Text search query
   * @param options - Search options
   */
  hybridSearch(
    table: string,
    vectorColumn: string,
    queryVector: number[],
    textColumn: string,
    textQuery: string,
    options?: HybridSearchOptions
  ): Promise<HybridSearchResult[]>;
}

// Vector Search Types

export interface VectorSearchOptions {
  /** Number of results to return (default: 10) */
  k?: number;
  /** Distance metric (cosine, euclidean, dot_product, manhattan) */
  metric?: 'cosine' | 'euclidean' | 'dot_product' | 'inner_product' | 'manhattan';
  /** SQL WHERE clause for filtering */
  filter?: string;
  /** HNSW ef_search parameter */
  efSearch?: number;
  /** IVF nprobe parameter */
  nprobe?: number;
}

export interface VectorSearchResult {
  id: any;
  distance: number;
  data: Record<string, any>;
}

export interface HybridSearchOptions {
  /** Number of results to return (default: 10) */
  k?: number;
  /** Distance metric (default: cosine) */
  metric?: 'cosine' | 'euclidean' | 'dot_product' | 'inner_product' | 'manhattan';
  /** Weight for vector results (0-1, default: 0.5) */
  vectorWeight?: number;
  /** Fusion method (rrf, linear) */
  fusion?: 'rrf' | 'linear';
  /** RRF k parameter (default: 60) */
  rrfK?: number;
  /** SQL WHERE clause for filtering */
  filter?: string;
}

export interface HybridSearchResult {
  id: any;
  score: number;
  data: Record<string, any>;
}

// Vector Utility Functions

/**
 * Calculate cosine similarity between two vectors.
 * @param a - First vector
 * @param b - Second vector
 * @returns Cosine similarity (-1 to 1)
 */
export function cosineSimilarity(a: number[], b: number[]): number;

/**
 * Calculate Euclidean distance between two vectors.
 * @param a - First vector
 * @param b - Second vector
 * @returns Euclidean distance
 */
export function euclideanDistance(a: number[], b: number[]): number;

/**
 * Calculate dot product of two vectors.
 * @param a - First vector
 * @param b - Second vector
 * @returns Dot product
 */
export function dotProduct(a: number[], b: number[]): number;

/**
 * Calculate Manhattan distance between two vectors.
 * @param a - First vector
 * @param b - Second vector
 * @returns Manhattan distance
 */
export function manhattanDistance(a: number[], b: number[]): number;

/**
 * Normalize a vector to unit length.
 * @param vector - Input vector
 * @returns Normalized vector
 */
export function normalizeVector(vector: number[]): number[];

// Text Chunking

export enum ChunkingStrategy {
  FIXED_SIZE = 'fixed_size',
  SENTENCE = 'sentence',
  PARAGRAPH = 'paragraph',
  SEMANTIC = 'semantic',
}

export interface ChunkOptions {
  /** Chunking strategy (default: fixed_size) */
  strategy?: ChunkingStrategy | string;
  /** Target chunk size in characters (default: 512) */
  chunkSize?: number;
  /** Overlap between chunks (default: 50) */
  overlap?: number;
}

export interface TextChunk {
  text: string;
  start: number;
  end: number;
}

/**
 * Chunk text into smaller pieces for embedding.
 * @param text - Text to chunk
 * @param options - Chunking options
 */
export function chunkText(text: string, options?: ChunkOptions): TextChunk[];

// Embedding Models

export interface EmbeddingModel {
  name: string;
  provider: 'openai' | 'huggingface' | 'cohere';
  dimensions: number;
  maxTokens: number;
}

/**
 * Common embedding models with their configurations.
 */
export const EmbeddingModels: Record<string, EmbeddingModel>;

/**
 * Get embedding model configuration.
 * @param modelName - Model name
 * @returns Model configuration or null if not found
 */
export function getEmbeddingModel(modelName: string): EmbeddingModel | null;

/**
 * List all available embedding models.
 * @returns Array of model configurations
 */
export function listEmbeddingModels(): EmbeddingModel[];
