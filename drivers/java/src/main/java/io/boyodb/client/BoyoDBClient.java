package io.boyodb.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * High-performance Java client for BoyoDB analytical database.
 *
 * <p>Features:
 * <ul>
 *   <li>Connection pooling for concurrent access</li>
 *   <li>Binary protocol with Arrow IPC support</li>
 *   <li>TLS encryption with certificate verification</li>
 *   <li>Automatic retry on connection failures</li>
 *   <li>Transaction support with savepoints</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * BoyoDBConfig config = BoyoDBConfig.builder()
 *     .host("localhost")
 *     .port(8765)
 *     .poolSize(10)
 *     .build();
 *
 * try (BoyoDBClient client = new BoyoDBClient(config)) {
 *     QueryResult result = client.query("SELECT * FROM mydb.users LIMIT 10");
 *     for (Map<String, Object> row : result) {
 *         System.out.println(row);
 *     }
 * }
 * }</pre>
 */
public class BoyoDBClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BoyoDBClient.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int MAX_FRAME_SIZE = 100 * 1024 * 1024; // 100MB

    private final BoyoDBConfig config;
    private final BlockingQueue<PooledConnection> connectionPool;
    private final BufferAllocator allocator;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private String sessionId;
    private String defaultDatabase;

    /**
     * Create a new BoyoDB client with the given configuration.
     *
     * @param config Client configuration
     * @throws BoyoDBException if initial connection fails
     */
    public BoyoDBClient(BoyoDBConfig config) throws BoyoDBException {
        this.config = config;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.connectionPool = new ArrayBlockingQueue<>(config.getPoolSize());
        this.defaultDatabase = config.getDatabase();

        // Initialize connection pool
        try {
            for (int i = 0; i < config.getPoolSize(); i++) {
                connectionPool.offer(createConnection());
            }
        } catch (Exception e) {
            close();
            throw new BoyoDBException("Failed to initialize connection pool", e);
        }

        // Verify with health check
        try {
            health();
        } catch (Exception e) {
            close();
            throw new BoyoDBException("Health check failed", e);
        }
    }

    /**
     * Create a new BoyoDB client with default configuration.
     *
     * @param host Server host
     * @param port Server port
     * @throws BoyoDBException if connection fails
     */
    public BoyoDBClient(String host, int port) throws BoyoDBException {
        this(BoyoDBConfig.builder().host(host).port(port).build());
    }

    private PooledConnection createConnection() throws BoyoDBException {
        Socket socket = null;
        try {
            if (config.isTls()) {
                SSLSocketFactory factory = createSSLSocketFactory();
                socket = factory.createSocket();
            } else {
                socket = new Socket();
            }

            socket.connect(
                new InetSocketAddress(config.getHost(), config.getPort()),
                (int) config.getConnectTimeout().toMillis()
            );
            socket.setSoTimeout((int) config.getReadTimeout().toMillis());
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);

            return new PooledConnection(socket);
        } catch (Exception e) {
            if (socket != null) {
                try { socket.close(); } catch (IOException ignored) {}
            }
            throw new BoyoDBException("Failed to create connection", e);
        }
    }

    private SSLSocketFactory createSSLSocketFactory() throws BoyoDBException {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");

            if (config.getCaFile() != null) {
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                try (FileInputStream fis = new FileInputStream(config.getCaFile())) {
                    X509Certificate ca = (X509Certificate) cf.generateCertificate(fis);
                    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
                    ks.load(null, null);
                    ks.setCertificateEntry("ca", ca);

                    TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                        TrustManagerFactory.getDefaultAlgorithm()
                    );
                    tmf.init(ks);
                    sslContext.init(null, tmf.getTrustManagers(), null);
                }
            } else {
                sslContext.init(null, null, null);
            }

            return sslContext.getSocketFactory();
        } catch (Exception e) {
            throw new BoyoDBException("Failed to create SSL context", e);
        }
    }

    private PooledConnection borrowConnection() throws BoyoDBException {
        if (closed.get()) {
            throw new BoyoDBException("Client is closed");
        }

        try {
            PooledConnection conn = connectionPool.poll(
                config.getConnectTimeout().toMillis(),
                TimeUnit.MILLISECONDS
            );
            if (conn == null) {
                throw new BoyoDBException("Connection pool exhausted");
            }
            if (!conn.isValid()) {
                conn.close();
                conn = createConnection();
            }
            return conn;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BoyoDBException("Interrupted while waiting for connection", e);
        }
    }

    private void returnConnection(PooledConnection conn) {
        if (conn != null && conn.isValid() && !closed.get()) {
            connectionPool.offer(conn);
        } else if (conn != null) {
            conn.close();
            // Replace with a new connection if pool isn't closing
            if (!closed.get()) {
                try {
                    connectionPool.offer(createConnection());
                } catch (Exception e) {
                    log.warn("Failed to replace connection in pool", e);
                }
            }
        }
    }

    private JsonNode sendRequest(ObjectNode request) throws BoyoDBException {
        return sendRequest(request, null);
    }

    private JsonNode sendRequest(ObjectNode request, byte[] binaryPayload) throws BoyoDBException {
        // Add auth
        if (sessionId != null) {
            request.put("auth", sessionId);
        } else if (config.getToken() != null) {
            request.put("auth", config.getToken());
        }

        BoyoDBException lastError = null;
        for (int attempt = 0; attempt < config.getMaxRetries(); attempt++) {
            PooledConnection conn = null;
            try {
                conn = borrowConnection();
                JsonNode response = sendRequestOnce(conn, request, binaryPayload);
                returnConnection(conn);
                return response;
            } catch (BoyoDBException e) {
                lastError = e;
                if (conn != null) {
                    conn.close();
                }
                if (attempt < config.getMaxRetries() - 1) {
                    try {
                        Thread.sleep(config.getRetryDelay().toMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new BoyoDBException("Interrupted during retry", ie);
                    }
                }
            }
        }
        throw new BoyoDBException("Failed after " + config.getMaxRetries() + " retries", lastError);
    }

    private JsonNode sendRequestOnce(PooledConnection conn, ObjectNode request, byte[] binaryPayload)
            throws BoyoDBException {
        try {
            DataOutputStream out = conn.getOutputStream();
            DataInputStream in = conn.getInputStream();

            // Serialize and send request
            byte[] jsonBytes = mapper.writeValueAsBytes(request);
            out.writeInt(jsonBytes.length);
            out.write(jsonBytes);

            // Send binary payload if present
            if (binaryPayload != null) {
                out.writeInt(binaryPayload.length);
                out.write(binaryPayload);
            }
            out.flush();

            // Read response length
            int respLen = in.readInt();
            if (respLen > MAX_FRAME_SIZE) {
                throw new BoyoDBException("Response too large: " + respLen);
            }

            // Read response body
            byte[] respBytes = new byte[respLen];
            in.readFully(respBytes);

            JsonNode response = mapper.readTree(respBytes);

            // Handle streaming IPC
            if (response.has("ipc_streaming") && response.get("ipc_streaming").asBoolean()) {
                ByteArrayOutputStream ipcBuffer = new ByteArrayOutputStream();
                while (true) {
                    int chunkLen = in.readInt();
                    if (chunkLen == 0) break;
                    if (chunkLen > MAX_FRAME_SIZE) {
                        throw new BoyoDBException("IPC chunk too large: " + chunkLen);
                    }
                    byte[] chunk = new byte[chunkLen];
                    in.readFully(chunk);
                    ipcBuffer.write(chunk);
                }
                ((ObjectNode) response).put("ipc_bytes_data", ipcBuffer.toByteArray());
            }
            // Handle fixed-length IPC
            else if (response.has("ipc_len")) {
                long ipcLen = response.get("ipc_len").asLong();
                if (ipcLen > MAX_FRAME_SIZE) {
                    throw new BoyoDBException("IPC payload too large: " + ipcLen);
                }
                int payloadLen = in.readInt();
                if (payloadLen != ipcLen) {
                    throw new BoyoDBException("IPC length mismatch: expected " + ipcLen + " got " + payloadLen);
                }
                byte[] ipcBytes = new byte[payloadLen];
                in.readFully(ipcBytes);
                ((ObjectNode) response).put("ipc_bytes_data", ipcBytes);
            }

            return response;
        } catch (IOException e) {
            throw new BoyoDBException("I/O error", e);
        }
    }

    /**
     * Check server health.
     *
     * @throws BoyoDBException if health check fails
     */
    public void health() throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "health");

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Health check failed: " + response.path("message").asText());
        }
    }

    /**
     * Login with username and password.
     *
     * @param username Username
     * @param password Password
     * @throws BoyoDBException if login fails
     */
    public void login(String username, String password) throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "login");
        request.put("username", username);
        request.put("password", password);

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Login failed: " + response.path("message").asText());
        }
        sessionId = response.path("session_id").asText();
    }

    /**
     * Logout from the server.
     *
     * @throws BoyoDBException if logout fails
     */
    public void logout() throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "logout");

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Logout failed: " + response.path("message").asText());
        }
        sessionId = null;
    }

    /**
     * Execute a SQL query and return results.
     *
     * @param sql SQL query string
     * @return Query result with rows
     * @throws BoyoDBException if query fails
     */
    public QueryResult query(String sql) throws BoyoDBException {
        return query(sql, defaultDatabase, config.getQueryTimeout());
    }

    /**
     * Execute a SQL query with options.
     *
     * @param sql SQL query string
     * @param database Database to use
     * @param timeoutMillis Query timeout in milliseconds
     * @return Query result with rows
     * @throws BoyoDBException if query fails
     */
    public QueryResult query(String sql, String database, long timeoutMillis) throws BoyoDBException {
        boolean isBinary = isSelectLike(sql);

        ObjectNode request = mapper.createObjectNode();
        request.put("op", isBinary ? "query_binary" : "query");
        request.put("sql", sql);
        request.put("timeout_millis", timeoutMillis);
        if (isBinary) {
            request.put("stream", true);
        }
        if (database != null) {
            request.put("database", database);
        }

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Query failed: " + response.path("message").asText());
        }

        QueryResult result = new QueryResult();
        result.setSegmentsScanned(response.path("segments_scanned").asInt(0));
        result.setDataSkippedBytes(response.path("data_skipped_bytes").asLong(0));

        // Parse Arrow IPC if present
        if (response.has("ipc_bytes_data")) {
            byte[] ipcBytes = (byte[]) ((ObjectNode) response).get("ipc_bytes_data").binaryValue();
            parseArrowIPC(ipcBytes, result);
        } else if (response.has("ipc_base64")) {
            byte[] ipcBytes = Base64.getDecoder().decode(response.get("ipc_base64").asText());
            parseArrowIPC(ipcBytes, result);
        }

        return result;
    }

    /**
     * Execute a SQL statement that doesn't return rows.
     *
     * @param sql SQL statement
     * @throws BoyoDBException if execution fails
     */
    public void exec(String sql) throws BoyoDBException {
        exec(sql, defaultDatabase, config.getQueryTimeout());
    }

    /**
     * Execute a SQL statement with options.
     *
     * @param sql SQL statement
     * @param database Database to use
     * @param timeoutMillis Execution timeout in milliseconds
     * @throws BoyoDBException if execution fails
     */
    public void exec(String sql, String database, long timeoutMillis) throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "query");
        request.put("sql", sql);
        request.put("timeout_millis", timeoutMillis);
        if (database != null) {
            request.put("database", database);
        }

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Exec failed: " + response.path("message").asText());
        }
    }

    /**
     * Create a new database.
     *
     * @param name Database name
     * @throws BoyoDBException if creation fails
     */
    public void createDatabase(String name) throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "createdatabase");
        request.put("name", name);

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Create database failed: " + response.path("message").asText());
        }
    }

    /**
     * Create a new table.
     *
     * @param database Database name
     * @param table Table name
     * @throws BoyoDBException if creation fails
     */
    public void createTable(String database, String table) throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "createtable");
        request.put("database", database);
        request.put("table", table);

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Create table failed: " + response.path("message").asText());
        }
    }

    /**
     * List all databases.
     *
     * @return List of database names
     * @throws BoyoDBException if listing fails
     */
    public List<String> listDatabases() throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "listdatabases");

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("List databases failed: " + response.path("message").asText());
        }

        List<String> databases = new ArrayList<>();
        response.path("databases").forEach(node -> databases.add(node.asText()));
        return databases;
    }

    /**
     * List tables in a database.
     *
     * @param database Database name (null for all)
     * @return List of table info
     * @throws BoyoDBException if listing fails
     */
    public List<TableInfo> listTables(String database) throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "listtables");
        if (database != null) {
            request.put("database", database);
        }

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("List tables failed: " + response.path("message").asText());
        }

        List<TableInfo> tables = new ArrayList<>();
        response.path("tables").forEach(node -> {
            TableInfo info = new TableInfo();
            info.setDatabase(node.path("database").asText());
            info.setName(node.path("name").asText());
            if (node.has("schema_json")) {
                info.setSchemaJson(node.path("schema_json").asText());
            }
            tables.add(info);
        });
        return tables;
    }

    /**
     * Get query execution plan.
     *
     * @param sql SQL query
     * @return Execution plan as JSON string
     * @throws BoyoDBException if explain fails
     */
    public String explain(String sql) throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "explain");
        request.put("sql", sql);

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Explain failed: " + response.path("message").asText());
        }

        return response.path("explain_plan").toString();
    }

    /**
     * Get server metrics.
     *
     * @return Metrics as JSON string
     * @throws BoyoDBException if metrics retrieval fails
     */
    public String metrics() throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "metrics");

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Metrics failed: " + response.path("message").asText());
        }

        return response.path("metrics").toString();
    }

    /**
     * Ingest CSV data into a table.
     *
     * @param database Database name
     * @param table Table name
     * @param csvData CSV data as bytes
     * @param hasHeader Whether CSV has header row
     * @param delimiter Field delimiter
     * @throws BoyoDBException if ingestion fails
     */
    public void ingestCSV(String database, String table, byte[] csvData,
                          boolean hasHeader, String delimiter) throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "ingestcsv");
        request.put("database", database);
        request.put("table", table);
        request.put("payload_base64", Base64.getEncoder().encodeToString(csvData));
        request.put("has_header", hasHeader);
        if (delimiter != null) {
            request.put("delimiter", delimiter);
        }

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Ingest CSV failed: " + response.path("message").asText());
        }
    }

    /**
     * Ingest Arrow IPC data into a table.
     *
     * @param database Database name
     * @param table Table name
     * @param ipcData Arrow IPC data
     * @throws BoyoDBException if ingestion fails
     */
    public void ingestIPC(String database, String table, byte[] ipcData) throws BoyoDBException {
        // Try binary protocol first
        try {
            ObjectNode request = mapper.createObjectNode();
            request.put("op", "ingest_ipc_binary");
            request.put("database", database);
            request.put("table", table);

            JsonNode response = sendRequest(request, ipcData);
            if ("ok".equals(response.path("status").asText())) {
                return;
            }
        } catch (Exception ignored) {
            // Fall back to base64
        }

        // Fall back to base64 encoding
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "ingestipc");
        request.put("database", database);
        request.put("table", table);
        request.put("payload_base64", Base64.getEncoder().encodeToString(ipcData));

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Ingest IPC failed: " + response.path("message").asText());
        }
    }

    // Transaction support

    /**
     * Begin a transaction.
     *
     * @throws BoyoDBException if begin fails
     */
    public void begin() throws BoyoDBException {
        exec("BEGIN");
    }

    /**
     * Begin a transaction with isolation level.
     *
     * @param isolationLevel Isolation level (READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE)
     * @param readOnly Whether transaction is read-only
     * @throws BoyoDBException if begin fails
     */
    public void begin(String isolationLevel, boolean readOnly) throws BoyoDBException {
        StringBuilder sql = new StringBuilder();
        if (isolationLevel != null) {
            sql.append("START TRANSACTION ISOLATION LEVEL ").append(isolationLevel);
            if (readOnly) {
                sql.append(" READ ONLY");
            }
        } else if (readOnly) {
            sql.append("START TRANSACTION READ ONLY");
        } else {
            sql.append("BEGIN");
        }
        exec(sql.toString());
    }

    /**
     * Commit the current transaction.
     *
     * @throws BoyoDBException if commit fails
     */
    public void commit() throws BoyoDBException {
        exec("COMMIT");
    }

    /**
     * Rollback the current transaction.
     *
     * @throws BoyoDBException if rollback fails
     */
    public void rollback() throws BoyoDBException {
        exec("ROLLBACK");
    }

    /**
     * Rollback to a savepoint.
     *
     * @param savepoint Savepoint name
     * @throws BoyoDBException if rollback fails
     */
    public void rollbackToSavepoint(String savepoint) throws BoyoDBException {
        exec("ROLLBACK TO SAVEPOINT " + savepoint);
    }

    /**
     * Create a savepoint.
     *
     * @param name Savepoint name
     * @throws BoyoDBException if creation fails
     */
    public void savepoint(String name) throws BoyoDBException {
        exec("SAVEPOINT " + name);
    }

    /**
     * Release a savepoint.
     *
     * @param name Savepoint name
     * @throws BoyoDBException if release fails
     */
    public void releaseSavepoint(String name) throws BoyoDBException {
        exec("RELEASE SAVEPOINT " + name);
    }

    /**
     * Execute a function within a transaction.
     *
     * @param action Action to execute
     * @param <T> Return type
     * @return Result of the action
     * @throws BoyoDBException if transaction fails
     */
    public <T> T inTransaction(TransactionAction<T> action) throws BoyoDBException {
        begin();
        try {
            T result = action.execute();
            commit();
            return result;
        } catch (Exception e) {
            try {
                rollback();
            } catch (Exception rollbackError) {
                log.warn("Rollback failed", rollbackError);
            }
            if (e instanceof BoyoDBException) {
                throw (BoyoDBException) e;
            }
            throw new BoyoDBException("Transaction failed", e);
        }
    }

    /**
     * Set the default database.
     *
     * @param database Database name
     */
    public void setDatabase(String database) {
        this.defaultDatabase = database;
    }

    private boolean isSelectLike(String sql) {
        String trimmed = sql.trim().toLowerCase();
        return trimmed.startsWith("select ") || trimmed.startsWith("with ");
    }

    private void parseArrowIPC(byte[] ipcBytes, QueryResult result) throws BoyoDBException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(ipcBytes);
             ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<String> columns = new ArrayList<>();
            for (Field field : root.getSchema().getFields()) {
                columns.add(field.getName());
            }
            result.setColumns(columns);

            List<Map<String, Object>> rows = new ArrayList<>();
            while (reader.loadNextBatch()) {
                int rowCount = root.getRowCount();
                for (int row = 0; row < rowCount; row++) {
                    Map<String, Object> rowMap = new LinkedHashMap<>();
                    for (int col = 0; col < root.getFieldVectors().size(); col++) {
                        FieldVector vector = root.getFieldVectors().get(col);
                        String colName = columns.get(col);
                        Object value = vector.isNull(row) ? null : vector.getObject(row);
                        rowMap.put(colName, value);
                    }
                    rows.add(rowMap);
                }
            }
            result.setRows(rows);
        } catch (Exception e) {
            throw new BoyoDBException("Failed to parse Arrow IPC", e);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            PooledConnection conn;
            while ((conn = connectionPool.poll()) != null) {
                conn.close();
            }
            allocator.close();
        }
    }

    /**
     * Transaction action interface.
     */
    @FunctionalInterface
    public interface TransactionAction<T> {
        T execute() throws BoyoDBException;
    }

    /**
     * Pooled connection wrapper.
     */
    private static class PooledConnection {
        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;
        private volatile boolean valid = true;

        PooledConnection(Socket socket) throws IOException {
            this.socket = socket;
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        }

        DataInputStream getInputStream() {
            return in;
        }

        DataOutputStream getOutputStream() {
            return out;
        }

        boolean isValid() {
            return valid && socket.isConnected() && !socket.isClosed();
        }

        void close() {
            valid = false;
            try { socket.close(); } catch (IOException ignored) {}
        }
    }
}
