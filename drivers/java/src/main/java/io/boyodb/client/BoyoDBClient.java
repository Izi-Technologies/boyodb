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

    // ========================================================================
    // Pub/Sub Support
    // ========================================================================

    /**
     * Start listening on a notification channel.
     *
     * @param channel Channel name
     * @throws BoyoDBException if listen fails
     */
    public void listen(String channel) throws BoyoDBException {
        exec("LISTEN " + escapeIdentifier(channel));
    }

    /**
     * Stop listening on a notification channel.
     *
     * @param channel Channel name (use "*" to unlisten all)
     * @throws BoyoDBException if unlisten fails
     */
    public void unlisten(String channel) throws BoyoDBException {
        if ("*".equals(channel)) {
            exec("UNLISTEN *");
        } else {
            exec("UNLISTEN " + escapeIdentifier(channel));
        }
    }

    /**
     * Send a notification on a channel.
     *
     * @param channel Channel name
     * @param payload Optional payload (may be null)
     * @throws BoyoDBException if notify fails
     */
    public void notify(String channel, String payload) throws BoyoDBException {
        if (payload != null) {
            exec(String.format("NOTIFY %s, '%s'", escapeIdentifier(channel), escapeString(payload)));
        } else {
            exec("NOTIFY " + escapeIdentifier(channel));
        }
    }

    // ========================================================================
    // Trigger Management
    // ========================================================================

    /**
     * Create a database trigger.
     *
     * @param name Trigger name
     * @param table Table name
     * @param timing When to fire: "BEFORE", "AFTER", or "INSTEAD OF"
     * @param events Events to trigger on: "INSERT", "UPDATE", "DELETE", or combinations
     * @param function Function to execute
     * @param arguments Arguments to pass to function
     * @param forEachRow Whether to fire for each row (vs statement)
     * @param whenClause Optional WHEN condition
     * @param orReplace Whether to replace existing trigger
     * @throws BoyoDBException if creation fails
     */
    public void createTrigger(String name, String table, String timing, List<String> events,
                               String function, List<Object> arguments, boolean forEachRow,
                               String whenClause, boolean orReplace) throws BoyoDBException {
        StringBuilder sql = new StringBuilder();
        sql.append(orReplace ? "CREATE OR REPLACE TRIGGER " : "CREATE TRIGGER ");
        sql.append(escapeIdentifier(name)).append(" ");
        sql.append(timing.toUpperCase()).append(" ");
        sql.append(String.join(" OR ", events.stream().map(String::toUpperCase).toList())).append(" ");
        sql.append("ON ").append(escapeIdentifier(table)).append(" ");
        sql.append(forEachRow ? "FOR EACH ROW " : "FOR EACH STATEMENT ");

        if (whenClause != null && !whenClause.isEmpty()) {
            sql.append("WHEN (").append(whenClause).append(") ");
        }

        sql.append("EXECUTE FUNCTION ").append(escapeIdentifier(function)).append("(");
        if (arguments != null && !arguments.isEmpty()) {
            sql.append(arguments.stream()
                    .map(this::formatValue)
                    .reduce((a, b) -> a + ", " + b)
                    .orElse(""));
        }
        sql.append(")");

        exec(sql.toString());
    }

    /**
     * Drop a trigger.
     *
     * @param name Trigger name
     * @param table Table the trigger is on
     * @param ifExists Don't error if trigger doesn't exist
     * @throws BoyoDBException if drop fails
     */
    public void dropTrigger(String name, String table, boolean ifExists) throws BoyoDBException {
        String sql = String.format("DROP TRIGGER %s%s ON %s",
                ifExists ? "IF EXISTS " : "",
                escapeIdentifier(name),
                escapeIdentifier(table));
        exec(sql);
    }

    /**
     * Enable or disable a trigger.
     *
     * @param name Trigger name
     * @param table Table the trigger is on
     * @param enable True to enable, false to disable
     * @throws BoyoDBException if alter fails
     */
    public void alterTrigger(String name, String table, boolean enable) throws BoyoDBException {
        String action = enable ? "ENABLE" : "DISABLE";
        exec(String.format("ALTER TRIGGER %s ON %s %s",
                escapeIdentifier(name), escapeIdentifier(table), action));
    }

    /**
     * List triggers on a table or in a database.
     *
     * @param table Table name (optional)
     * @param database Database name (optional)
     * @return Query result with trigger information
     * @throws BoyoDBException if listing fails
     */
    public QueryResult listTriggers(String table, String database) throws BoyoDBException {
        String sql = "SHOW TRIGGERS";
        if (table != null && !table.isEmpty()) {
            sql += " ON " + escapeIdentifier(table);
        } else if (database != null && !database.isEmpty()) {
            sql += " IN " + escapeIdentifier(database);
        }
        return query(sql);
    }

    // ========================================================================
    // Stored Procedures and Functions
    // ========================================================================

    /**
     * Call a stored procedure.
     *
     * @param procedure Procedure name
     * @param args Arguments to pass
     * @return Query result
     * @throws BoyoDBException if call fails
     */
    public QueryResult call(String procedure, Object... args) throws BoyoDBException {
        StringBuilder sql = new StringBuilder("CALL ");
        sql.append(escapeIdentifier(procedure)).append("(");
        for (int i = 0; i < args.length; i++) {
            if (i > 0) sql.append(", ");
            sql.append(formatValue(args[i]));
        }
        sql.append(")");
        return query(sql.toString());
    }

    /**
     * Create a stored function.
     *
     * @param name Function name
     * @param parameters List of (name, type) pairs
     * @param returnType Return type
     * @param body Function body
     * @param language Language (default: plpgsql)
     * @param orReplace Whether to replace existing function
     * @param volatility IMMUTABLE, STABLE, or VOLATILE (optional)
     * @throws BoyoDBException if creation fails
     */
    public void createFunction(String name, List<Map.Entry<String, String>> parameters,
                                String returnType, String body, String language,
                                boolean orReplace, String volatility) throws BoyoDBException {
        if (language == null) language = "plpgsql";

        StringBuilder sql = new StringBuilder();
        sql.append(orReplace ? "CREATE OR REPLACE FUNCTION " : "CREATE FUNCTION ");
        sql.append(escapeIdentifier(name)).append("(");
        for (int i = 0; i < parameters.size(); i++) {
            if (i > 0) sql.append(", ");
            Map.Entry<String, String> param = parameters.get(i);
            sql.append(escapeIdentifier(param.getKey())).append(" ").append(param.getValue());
        }
        sql.append(") RETURNS ").append(returnType);
        sql.append(" LANGUAGE ").append(language).append(" ");

        if (volatility != null && !volatility.isEmpty()) {
            sql.append(volatility.toUpperCase()).append(" ");
        }

        sql.append("AS $$ ").append(body).append(" $$");

        exec(sql.toString());
    }

    /**
     * Create a stored procedure.
     *
     * @param name Procedure name
     * @param parameters List of (name, type, mode) tuples where mode is IN, OUT, or INOUT
     * @param body Procedure body
     * @param language Language (default: plpgsql)
     * @param orReplace Whether to replace existing procedure
     * @throws BoyoDBException if creation fails
     */
    public void createProcedure(String name, List<ProcedureParameter> parameters,
                                 String body, String language, boolean orReplace) throws BoyoDBException {
        if (language == null) language = "plpgsql";

        StringBuilder sql = new StringBuilder();
        sql.append(orReplace ? "CREATE OR REPLACE PROCEDURE " : "CREATE PROCEDURE ");
        sql.append(escapeIdentifier(name)).append("(");
        for (int i = 0; i < parameters.size(); i++) {
            if (i > 0) sql.append(", ");
            ProcedureParameter param = parameters.get(i);
            if (param.mode != null && !param.mode.isEmpty()) {
                sql.append(param.mode.toUpperCase()).append(" ");
            }
            sql.append(escapeIdentifier(param.name)).append(" ").append(param.type);
        }
        sql.append(") LANGUAGE ").append(language);
        sql.append(" AS $$ ").append(body).append(" $$");

        exec(sql.toString());
    }

    /**
     * Drop a function.
     *
     * @param name Function name
     * @param paramTypes Parameter types for overload resolution (may be null)
     * @param ifExists Don't error if function doesn't exist
     * @throws BoyoDBException if drop fails
     */
    public void dropFunction(String name, List<String> paramTypes, boolean ifExists) throws BoyoDBException {
        StringBuilder sql = new StringBuilder("DROP FUNCTION ");
        if (ifExists) sql.append("IF EXISTS ");
        sql.append(escapeIdentifier(name));
        if (paramTypes != null && !paramTypes.isEmpty()) {
            sql.append("(").append(String.join(", ", paramTypes)).append(")");
        }
        exec(sql.toString());
    }

    /**
     * Drop a procedure.
     *
     * @param name Procedure name
     * @param paramTypes Parameter types for overload resolution (may be null)
     * @param ifExists Don't error if procedure doesn't exist
     * @throws BoyoDBException if drop fails
     */
    public void dropProcedure(String name, List<String> paramTypes, boolean ifExists) throws BoyoDBException {
        StringBuilder sql = new StringBuilder("DROP PROCEDURE ");
        if (ifExists) sql.append("IF EXISTS ");
        sql.append(escapeIdentifier(name));
        if (paramTypes != null && !paramTypes.isEmpty()) {
            sql.append("(").append(String.join(", ", paramTypes)).append(")");
        }
        exec(sql.toString());
    }

    /**
     * Procedure parameter with mode.
     */
    public static class ProcedureParameter {
        public final String name;
        public final String type;
        public final String mode; // IN, OUT, INOUT

        public ProcedureParameter(String name, String type, String mode) {
            this.name = name;
            this.type = type;
            this.mode = mode;
        }

        public ProcedureParameter(String name, String type) {
            this(name, type, null);
        }
    }

    // ========================================================================
    // JSON Operations
    // ========================================================================

    /**
     * Extract a JSON value using the -> operator.
     *
     * @param column JSON column name
     * @param path JSON key or array index
     * @param table Table name
     * @param where Optional WHERE clause
     * @param limit Optional LIMIT
     * @return Query result
     * @throws BoyoDBException if query fails
     */
    public QueryResult jsonExtract(String column, String path, String table,
                                    String where, Integer limit) throws BoyoDBException {
        String pathExpr = path.matches("\\d+") ? path : "'" + escapeString(path) + "'";
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(escapeIdentifier(column)).append(" -> ").append(pathExpr);
        sql.append(" AS value FROM ").append(escapeIdentifier(table));
        if (where != null && !where.isEmpty()) sql.append(" WHERE ").append(where);
        if (limit != null && limit > 0) sql.append(" LIMIT ").append(limit);
        return query(sql.toString());
    }

    /**
     * Extract a JSON value as text using the ->> operator.
     *
     * @param column JSON column name
     * @param path JSON key or array index
     * @param table Table name
     * @param where Optional WHERE clause
     * @param limit Optional LIMIT
     * @return Query result
     * @throws BoyoDBException if query fails
     */
    public QueryResult jsonExtractText(String column, String path, String table,
                                        String where, Integer limit) throws BoyoDBException {
        String pathExpr = path.matches("\\d+") ? path : "'" + escapeString(path) + "'";
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(escapeIdentifier(column)).append(" ->> ").append(pathExpr);
        sql.append(" AS value FROM ").append(escapeIdentifier(table));
        if (where != null && !where.isEmpty()) sql.append(" WHERE ").append(where);
        if (limit != null && limit > 0) sql.append(" LIMIT ").append(limit);
        return query(sql.toString());
    }

    /**
     * Extract a nested JSON value using the #> operator.
     *
     * @param column JSON column name
     * @param path Array of path elements
     * @param table Table name
     * @param where Optional WHERE clause
     * @param limit Optional LIMIT
     * @return Query result
     * @throws BoyoDBException if query fails
     */
    public QueryResult jsonExtractPath(String column, List<String> path, String table,
                                        String where, Integer limit) throws BoyoDBException {
        String pathArray = "'{" + path.stream()
                .map(this::escapeString)
                .reduce((a, b) -> a + "," + b)
                .orElse("") + "}'";
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(escapeIdentifier(column)).append(" #> ").append(pathArray);
        sql.append(" AS value FROM ").append(escapeIdentifier(table));
        if (where != null && !where.isEmpty()) sql.append(" WHERE ").append(where);
        if (limit != null && limit > 0) sql.append(" LIMIT ").append(limit);
        return query(sql.toString());
    }

    /**
     * Check if JSON contains a value using the @> operator.
     *
     * @param column JSON column name
     * @param value Value to check for
     * @param table Table name
     * @param where Optional additional WHERE clause
     * @param limit Optional LIMIT
     * @return Query result
     * @throws BoyoDBException if query fails
     */
    public QueryResult jsonContains(String column, Object value, String table,
                                     String where, Integer limit) throws BoyoDBException {
        String jsonValue;
        try {
            jsonValue = mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new BoyoDBException("Failed to serialize JSON value", e);
        }
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(escapeIdentifier(table));
        sql.append(" WHERE ").append(escapeIdentifier(column)).append(" @> '");
        sql.append(escapeString(jsonValue)).append("'::jsonb");
        if (where != null && !where.isEmpty()) sql.append(" AND ").append(where);
        if (limit != null && limit > 0) sql.append(" LIMIT ").append(limit);
        return query(sql.toString());
    }

    /**
     * Check if JSON is contained in a value using the <@ operator.
     *
     * @param column JSON column name
     * @param value Value to check against
     * @param table Table name
     * @param where Optional additional WHERE clause
     * @param limit Optional LIMIT
     * @return Query result
     * @throws BoyoDBException if query fails
     */
    public QueryResult jsonContainedBy(String column, Object value, String table,
                                        String where, Integer limit) throws BoyoDBException {
        String jsonValue;
        try {
            jsonValue = mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new BoyoDBException("Failed to serialize JSON value", e);
        }
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(escapeIdentifier(table));
        sql.append(" WHERE ").append(escapeIdentifier(column)).append(" <@ '");
        sql.append(escapeString(jsonValue)).append("'::jsonb");
        if (where != null && !where.isEmpty()) sql.append(" AND ").append(where);
        if (limit != null && limit > 0) sql.append(" LIMIT ").append(limit);
        return query(sql.toString());
    }

    /**
     * Check if a JSON path exists using the @? operator.
     *
     * @param column JSON column name
     * @param jsonPath JSONPath expression
     * @param table Table name
     * @param where Optional additional WHERE clause
     * @param limit Optional LIMIT
     * @return Query result
     * @throws BoyoDBException if query fails
     */
    public QueryResult jsonPathExists(String column, String jsonPath, String table,
                                       String where, Integer limit) throws BoyoDBException {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(escapeIdentifier(table));
        sql.append(" WHERE ").append(escapeIdentifier(column)).append(" @? '");
        sql.append(escapeString(jsonPath)).append("'");
        if (where != null && !where.isEmpty()) sql.append(" AND ").append(where);
        if (limit != null && limit > 0) sql.append(" LIMIT ").append(limit);
        return query(sql.toString());
    }

    /**
     * Check if a JSON path predicate matches using the @@ operator.
     *
     * @param column JSON column name
     * @param jsonPath JSONPath predicate expression
     * @param table Table name
     * @param where Optional additional WHERE clause
     * @param limit Optional LIMIT
     * @return Query result
     * @throws BoyoDBException if query fails
     */
    public QueryResult jsonPathMatch(String column, String jsonPath, String table,
                                      String where, Integer limit) throws BoyoDBException {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(escapeIdentifier(table));
        sql.append(" WHERE ").append(escapeIdentifier(column)).append(" @@ '");
        sql.append(escapeString(jsonPath)).append("'");
        if (where != null && !where.isEmpty()) sql.append(" AND ").append(where);
        if (limit != null && limit > 0) sql.append(" LIMIT ").append(limit);
        return query(sql.toString());
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    private String escapeIdentifier(String identifier) {
        if (identifier.contains(".")) {
            return Arrays.stream(identifier.split("\\."))
                    .map(this::escapeIdentifier)
                    .reduce((a, b) -> a + "." + b)
                    .orElse(identifier);
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private String escapeString(String value) {
        return value.replace("'", "''");
    }

    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? "TRUE" : "FALSE";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof String) {
            return "'" + escapeString((String) value) + "'";
        }
        // Try JSON serialization for complex types
        try {
            return "'" + escapeString(mapper.writeValueAsString(value)) + "'";
        } catch (Exception e) {
            return "'" + escapeString(value.toString()) + "'";
        }
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

    // Prepared Statements

    /**
     * Prepare a SELECT query and return a prepared ID.
     *
     * @param sql SQL query
     * @param database Database to use
     * @return Prepared statement ID
     * @throws BoyoDBException if preparation fails
     */
    public String prepare(String sql, String database) throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "prepare");
        request.put("sql", sql);
        if (database != null) {
            request.put("database", database);
        }

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Prepare failed: " + response.path("message").asText());
        }

        String preparedId = response.path("prepared_id").asText();
        if (preparedId == null || preparedId.isEmpty()) {
            throw new BoyoDBException("Missing prepared_id in response");
        }
        return preparedId;
    }

    /**
     * Execute a prepared statement using binary IPC.
     *
     * @param preparedId Prepared statement ID
     * @param timeoutMillis Query timeout in milliseconds
     * @return Query result
     * @throws BoyoDBException if execution fails
     */
    public QueryResult executePreparedBinary(String preparedId, long timeoutMillis) throws BoyoDBException {
        ObjectNode request = mapper.createObjectNode();
        request.put("op", "execute_prepared_binary");
        request.put("id", preparedId);
        request.put("timeout_millis", timeoutMillis);
        request.put("stream", true);

        JsonNode response = sendRequest(request);
        if (!"ok".equals(response.path("status").asText())) {
            throw new BoyoDBException("Execute prepared failed: " + response.path("message").asText());
        }

        QueryResult result = new QueryResult();
        result.setSegmentsScanned(response.path("segments_scanned").asInt(0));
        result.setDataSkippedBytes(response.path("data_skipped_bytes").asLong(0));

        if (response.has("ipc_bytes_data")) {
            byte[] ipcBytes = (byte[]) ((ObjectNode) response).get("ipc_bytes_data").binaryValue();
            parseArrowIPC(ipcBytes, result);
        } else if (response.has("ipc_base64")) {
            byte[] ipcBytes = Base64.getDecoder().decode(response.get("ipc_base64").asText());
            parseArrowIPC(ipcBytes, result);
        }

        return result;
    }

    // Vector Search Support

    /**
     * Perform vector similarity search.
     *
     * @param queryVector Query embedding vector
     * @param table Table to search
     * @param vectorColumn Column containing embeddings
     * @param idColumn Column containing row IDs
     * @param metric Distance metric: "cosine", "euclidean", "dot", "manhattan"
     * @param limit Maximum number of results
     * @param filter Optional SQL WHERE clause
     * @param selectColumns Additional columns to return
     * @return Query result with matches
     * @throws BoyoDBException if search fails
     */
    public QueryResult vectorSearch(
            float[] queryVector,
            String table,
            String vectorColumn,
            String idColumn,
            String metric,
            int limit,
            String filter,
            List<String> selectColumns) throws BoyoDBException {

        if (vectorColumn == null) vectorColumn = "embedding";
        if (idColumn == null) idColumn = "id";
        if (metric == null) metric = "cosine";
        if (limit <= 0) limit = 10;

        StringBuilder selectCols = new StringBuilder(idColumn);
        if (selectColumns != null && !selectColumns.isEmpty()) {
            selectCols.append(", ").append(String.join(", ", selectColumns));
        }

        String vectorStr = formatVector(queryVector);

        String sql;
        String order;
        if ("cosine".equals(metric)) {
            sql = String.format("SELECT %s, vector_similarity(%s, %s) AS score FROM %s",
                    selectCols, vectorColumn, vectorStr, table);
            order = "DESC";
        } else {
            sql = String.format("SELECT %s, vector_distance(%s, %s, '%s') AS score FROM %s",
                    selectCols, vectorColumn, vectorStr, metric, table);
            order = "ASC";
        }

        if (filter != null && !filter.isEmpty()) {
            sql += " WHERE " + filter;
        }

        sql += " ORDER BY score " + order + " LIMIT " + limit;

        return query(sql);
    }

    /**
     * Perform hybrid search combining vector similarity and text search.
     *
     * @param queryVector Query embedding vector
     * @param textQuery Text query for BM25 search
     * @param table Table to search
     * @param vectorColumn Column containing embeddings
     * @param textColumn Column containing text
     * @param idColumn Column containing row IDs
     * @param vectorWeight Weight for vector similarity (0.0-1.0)
     * @param textWeight Weight for text relevance (0.0-1.0)
     * @param limit Maximum number of results
     * @param filter Optional SQL WHERE clause
     * @param selectColumns Additional columns to return
     * @return Query result with combined scores
     * @throws BoyoDBException if search fails
     */
    public QueryResult hybridSearch(
            float[] queryVector,
            String textQuery,
            String table,
            String vectorColumn,
            String textColumn,
            String idColumn,
            double vectorWeight,
            double textWeight,
            int limit,
            String filter,
            List<String> selectColumns) throws BoyoDBException {

        if (vectorColumn == null) vectorColumn = "embedding";
        if (textColumn == null) textColumn = "content";
        if (idColumn == null) idColumn = "id";
        if (vectorWeight <= 0) vectorWeight = 0.5;
        if (textWeight <= 0) textWeight = 0.5;
        if (limit <= 0) limit = 10;

        StringBuilder selectCols = new StringBuilder(idColumn);
        if (selectColumns != null && !selectColumns.isEmpty()) {
            selectCols.append(", ").append(String.join(", ", selectColumns));
        }

        String vectorStr = formatVector(queryVector);
        String escapedText = textQuery.replace("'", "''");

        String sql = String.format("""
                SELECT %s,
                       vector_similarity(%s, %s) * %.2f AS vector_score,
                       COALESCE(match_score(%s, '%s'), 0) * %.2f AS text_score,
                       vector_similarity(%s, %s) * %.2f + COALESCE(match_score(%s, '%s'), 0) * %.2f AS combined_score
                FROM %s
                """,
                selectCols,
                vectorColumn, vectorStr, vectorWeight,
                textColumn, escapedText, textWeight,
                vectorColumn, vectorStr, vectorWeight,
                textColumn, escapedText, textWeight,
                table);

        if (filter != null && !filter.isEmpty()) {
            sql += " WHERE " + filter;
        }

        sql += " ORDER BY combined_score DESC LIMIT " + limit;

        return query(sql);
    }

    private String formatVector(float[] vector) {
        StringBuilder sb = new StringBuilder("ARRAY[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(vector[i]);
        }
        sb.append("]");
        return sb.toString();
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

    // ========================================================================
    // Cursor Support
    // ========================================================================

    /**
     * Declare a server-side cursor.
     *
     * @param name Cursor name
     * @param queryStr SQL query for the cursor
     * @param scroll Allow backward movement
     * @param hold Keep cursor open after commit
     * @throws BoyoDBException if declaration fails
     */
    public void declareCursor(String name, String queryStr, boolean scroll, boolean hold) throws BoyoDBException {
        StringBuilder sql = new StringBuilder("DECLARE ");
        sql.append(escapeIdentifier(name));
        if (scroll) sql.append(" SCROLL");
        if (hold) sql.append(" WITH HOLD");
        sql.append(" CURSOR FOR ").append(queryStr);
        exec(sql.toString());
    }

    /**
     * Fetch rows from a cursor.
     *
     * @param name Cursor name
     * @param count Number of rows to fetch
     * @param direction Fetch direction (NEXT, PRIOR, FIRST, LAST, ABSOLUTE n, RELATIVE n)
     * @return Query result with fetched rows
     * @throws BoyoDBException if fetch fails
     */
    public QueryResult fetchCursor(String name, int count, String direction) throws BoyoDBException {
        if (direction == null || direction.isEmpty()) direction = "NEXT";
        String dir = direction.toUpperCase();
        String sql;
        if (dir.equals("NEXT") || dir.equals("PRIOR") || dir.equals("FIRST") || dir.equals("LAST")) {
            sql = String.format("FETCH %s %d FROM %s", dir, count, escapeIdentifier(name));
        } else {
            sql = String.format("FETCH %s FROM %s", dir, escapeIdentifier(name));
        }
        return query(sql);
    }

    /**
     * Move cursor position without returning data.
     *
     * @param name Cursor name
     * @param count Number of rows to move
     * @param direction Move direction
     * @throws BoyoDBException if move fails
     */
    public void moveCursor(String name, int count, String direction) throws BoyoDBException {
        if (direction == null || direction.isEmpty()) direction = "NEXT";
        String dir = direction.toUpperCase();
        String sql;
        if (dir.equals("NEXT") || dir.equals("PRIOR") || dir.equals("FIRST") || dir.equals("LAST")) {
            sql = String.format("MOVE %s %d IN %s", dir, count, escapeIdentifier(name));
        } else {
            sql = String.format("MOVE %s IN %s", dir, escapeIdentifier(name));
        }
        exec(sql);
    }

    /**
     * Close a cursor.
     *
     * @param name Cursor name (use "*" or "ALL" to close all cursors)
     * @throws BoyoDBException if close fails
     */
    public void closeCursor(String name) throws BoyoDBException {
        if ("*".equals(name) || "ALL".equalsIgnoreCase(name)) {
            exec("CLOSE ALL");
        } else {
            exec("CLOSE " + escapeIdentifier(name));
        }
    }

    // ========================================================================
    // Large Objects (LOB) Support
    // ========================================================================

    /**
     * Create a new large object.
     *
     * @return OID of the new large object
     * @throws BoyoDBException if creation fails
     */
    public long loCreate() throws BoyoDBException {
        QueryResult result = query("SELECT lo_create(0) AS oid");
        if (result.getRows().isEmpty()) {
            throw new BoyoDBException("No result from lo_create");
        }
        return ((Number) result.getRows().get(0).get("oid")).longValue();
    }

    /**
     * Open a large object.
     *
     * @param oid OID of the large object
     * @param mode Open mode ("r", "w", "rw")
     * @return File descriptor
     * @throws BoyoDBException if open fails
     */
    public int loOpen(long oid, String mode) throws BoyoDBException {
        // INV_READ = 0x40000, INV_WRITE = 0x20000
        int modeFlag = 0x40000; // read
        if (mode != null && mode.contains("w")) {
            modeFlag |= 0x20000;
        }
        QueryResult result = query(String.format("SELECT lo_open(%d, %d) AS fd", oid, modeFlag));
        if (result.getRows().isEmpty()) {
            throw new BoyoDBException("No result from lo_open");
        }
        return ((Number) result.getRows().get(0).get("fd")).intValue();
    }

    /**
     * Write data to a large object.
     *
     * @param fd File descriptor from loOpen
     * @param data Data to write
     * @return Number of bytes written
     * @throws BoyoDBException if write fails
     */
    public int loWrite(int fd, byte[] data) throws BoyoDBException {
        String b64 = Base64.getEncoder().encodeToString(data);
        QueryResult result = query(String.format("SELECT lowrite(%d, decode('%s', 'base64')) AS written", fd, b64));
        if (result.getRows().isEmpty()) {
            throw new BoyoDBException("No result from lowrite");
        }
        return ((Number) result.getRows().get(0).get("written")).intValue();
    }

    /**
     * Read data from a large object.
     *
     * @param fd File descriptor from loOpen
     * @param length Number of bytes to read
     * @return Data read
     * @throws BoyoDBException if read fails
     */
    public byte[] loRead(int fd, int length) throws BoyoDBException {
        QueryResult result = query(String.format("SELECT encode(loread(%d, %d), 'base64') AS data", fd, length));
        if (result.getRows().isEmpty()) {
            throw new BoyoDBException("No result from loread");
        }
        String b64 = (String) result.getRows().get(0).get("data");
        return Base64.getDecoder().decode(b64);
    }

    /**
     * Seek within a large object.
     *
     * @param fd File descriptor
     * @param offset Byte offset
     * @param whence 0=SEEK_SET, 1=SEEK_CUR, 2=SEEK_END
     * @return New position
     * @throws BoyoDBException if seek fails
     */
    public long loSeek(int fd, long offset, int whence) throws BoyoDBException {
        QueryResult result = query(String.format("SELECT lo_lseek(%d, %d, %d) AS pos", fd, offset, whence));
        if (result.getRows().isEmpty()) {
            throw new BoyoDBException("No result from lo_lseek");
        }
        return ((Number) result.getRows().get(0).get("pos")).longValue();
    }

    /**
     * Close a large object file descriptor.
     *
     * @param fd File descriptor
     * @throws BoyoDBException if close fails
     */
    public void loClose(int fd) throws BoyoDBException {
        query(String.format("SELECT lo_close(%d)", fd));
    }

    /**
     * Delete a large object.
     *
     * @param oid OID of the large object
     * @throws BoyoDBException if delete fails
     */
    public void loUnlink(long oid) throws BoyoDBException {
        query(String.format("SELECT lo_unlink(%d)", oid));
    }

    /**
     * Import data as a large object.
     *
     * @param data Data to import
     * @return OID of the new large object
     * @throws BoyoDBException if import fails
     */
    public long loImport(byte[] data) throws BoyoDBException {
        long oid = loCreate();
        int fd = loOpen(oid, "w");
        try {
            loWrite(fd, data);
        } finally {
            loClose(fd);
        }
        return oid;
    }

    /**
     * Export a large object as bytes.
     *
     * @param oid OID of the large object
     * @return Large object data
     * @throws BoyoDBException if export fails
     */
    public byte[] loExport(long oid) throws BoyoDBException {
        int fd = loOpen(oid, "r");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            while (true) {
                byte[] chunk = loRead(fd, 8192);
                if (chunk.length == 0) break;
                buffer.write(chunk, 0, chunk.length);
            }
        } finally {
            loClose(fd);
        }
        return buffer.toByteArray();
    }

    // ========================================================================
    // Advisory Locks
    // ========================================================================

    /**
     * Acquire a session-level advisory lock.
     *
     * @param key Lock key
     * @param shared Shared (true) or exclusive (false) lock
     * @throws BoyoDBException if lock acquisition fails
     */
    public void advisoryLock(long key, boolean shared) throws BoyoDBException {
        String fn = shared ? "pg_advisory_lock_shared" : "pg_advisory_lock";
        query(String.format("SELECT %s(%d)", fn, key));
    }

    /**
     * Try to acquire an advisory lock without blocking.
     *
     * @param key Lock key
     * @param shared Shared (true) or exclusive (false) lock
     * @return True if lock acquired, false otherwise
     * @throws BoyoDBException if query fails
     */
    public boolean advisoryLockTry(long key, boolean shared) throws BoyoDBException {
        String fn = shared ? "pg_try_advisory_lock_shared" : "pg_try_advisory_lock";
        QueryResult result = query(String.format("SELECT %s(%d) AS acquired", fn, key));
        if (result.getRows().isEmpty()) {
            return false;
        }
        return Boolean.TRUE.equals(result.getRows().get(0).get("acquired"));
    }

    /**
     * Release a session-level advisory lock.
     *
     * @param key Lock key
     * @param shared Shared (true) or exclusive (false) lock
     * @return True if lock was released
     * @throws BoyoDBException if query fails
     */
    public boolean advisoryUnlock(long key, boolean shared) throws BoyoDBException {
        String fn = shared ? "pg_advisory_unlock_shared" : "pg_advisory_unlock";
        QueryResult result = query(String.format("SELECT %s(%d) AS released", fn, key));
        if (result.getRows().isEmpty()) {
            return false;
        }
        return Boolean.TRUE.equals(result.getRows().get(0).get("released"));
    }

    /**
     * Release all session-level advisory locks.
     *
     * @throws BoyoDBException if query fails
     */
    public void advisoryUnlockAll() throws BoyoDBException {
        query("SELECT pg_advisory_unlock_all()");
    }

    /**
     * Acquire a transaction-level advisory lock.
     *
     * @param key Lock key
     * @param shared Shared (true) or exclusive (false) lock
     * @throws BoyoDBException if lock acquisition fails
     */
    public void advisoryXactLock(long key, boolean shared) throws BoyoDBException {
        String fn = shared ? "pg_advisory_xact_lock_shared" : "pg_advisory_xact_lock";
        query(String.format("SELECT %s(%d)", fn, key));
    }

    // ========================================================================
    // Full-Text Search
    // ========================================================================

    /**
     * Perform a full-text search.
     *
     * @param table Table name
     * @param textColumn Text column to search
     * @param queryText Search query
     * @param config Text search configuration (default: "english")
     * @param limit Maximum results
     * @param includeRank Include ranking
     * @param where Additional WHERE clause
     * @return Query result with matches
     * @throws BoyoDBException if search fails
     */
    public QueryResult ftsSearch(String table, String textColumn, String queryText,
                                  String config, int limit, boolean includeRank, String where) throws BoyoDBException {
        if (config == null || config.isEmpty()) config = "english";
        if (limit <= 0) limit = 100;

        String safeTable = escapeIdentifier(table);
        String safeColumn = escapeIdentifier(textColumn);
        String safeQuery = escapeString(queryText);

        StringBuilder sql = new StringBuilder();
        if (includeRank) {
            sql.append(String.format(
                "SELECT *, ts_rank(to_tsvector('%s', %s), plainto_tsquery('%s', '%s')) AS rank FROM %s WHERE to_tsvector('%s', %s) @@ plainto_tsquery('%s', '%s')",
                config, safeColumn, config, safeQuery, safeTable, config, safeColumn, config, safeQuery));
        } else {
            sql.append(String.format(
                "SELECT * FROM %s WHERE to_tsvector('%s', %s) @@ plainto_tsquery('%s', '%s')",
                safeTable, config, safeColumn, config, safeQuery));
        }
        if (where != null && !where.isEmpty()) sql.append(" AND ").append(where);
        if (includeRank) sql.append(" ORDER BY rank DESC");
        sql.append(" LIMIT ").append(limit);

        return query(sql.toString());
    }

    /**
     * Get highlighted search results.
     *
     * @param table Table name
     * @param textColumn Text column to search
     * @param queryText Search query
     * @param config Text search configuration
     * @param startTag Tag before match
     * @param stopTag Tag after match
     * @param limit Maximum results
     * @return Query result with highlighted text
     * @throws BoyoDBException if search fails
     */
    public QueryResult ftsHighlight(String table, String textColumn, String queryText,
                                     String config, String startTag, String stopTag, int limit) throws BoyoDBException {
        if (config == null || config.isEmpty()) config = "english";
        if (startTag == null) startTag = "<b>";
        if (stopTag == null) stopTag = "</b>";
        if (limit <= 0) limit = 100;

        String sql = String.format(
            "SELECT *, ts_headline('%s', %s, plainto_tsquery('%s', '%s'), 'StartSel=%s, StopSel=%s') AS headline FROM %s WHERE to_tsvector('%s', %s) @@ plainto_tsquery('%s', '%s') LIMIT %d",
            config, escapeIdentifier(textColumn), config, escapeString(queryText), startTag, stopTag,
            escapeIdentifier(table), config, escapeIdentifier(textColumn), config, escapeString(queryText), limit);

        return query(sql);
    }

    // ========================================================================
    // Geospatial Support
    // ========================================================================

    /**
     * Find records within a distance from a point.
     *
     * @param table Table name
     * @param geoColumn Geometry/geography column
     * @param lat Latitude
     * @param lon Longitude
     * @param radiusMeters Search radius in meters
     * @param limit Maximum results
     * @param where Additional WHERE clause
     * @return Query result with matches and distances
     * @throws BoyoDBException if search fails
     */
    public QueryResult geoDistance(String table, String geoColumn, double lat, double lon,
                                    double radiusMeters, int limit, String where) throws BoyoDBException {
        if (limit <= 0) limit = 100;

        String safeTable = escapeIdentifier(table);
        String safeColumn = escapeIdentifier(geoColumn);

        StringBuilder sql = new StringBuilder(String.format(
            "SELECT *, ST_Distance(%s, ST_SetSRID(ST_MakePoint(%f, %f), 4326)::geography) AS distance FROM %s WHERE ST_DWithin(%s, ST_SetSRID(ST_MakePoint(%f, %f), 4326)::geography, %f)",
            safeColumn, lon, lat, safeTable, safeColumn, lon, lat, radiusMeters));

        if (where != null && !where.isEmpty()) sql.append(" AND ").append(where);
        sql.append(" ORDER BY distance ASC LIMIT ").append(limit);

        return query(sql.toString());
    }

    /**
     * Find K nearest neighbors to a point.
     *
     * @param table Table name
     * @param geoColumn Geometry/geography column
     * @param lat Latitude
     * @param lon Longitude
     * @param k Number of neighbors
     * @param where Additional WHERE clause
     * @return Query result with nearest matches
     * @throws BoyoDBException if search fails
     */
    public QueryResult geoNearest(String table, String geoColumn, double lat, double lon,
                                   int k, String where) throws BoyoDBException {
        if (k <= 0) k = 10;

        String safeTable = escapeIdentifier(table);
        String safeColumn = escapeIdentifier(geoColumn);

        StringBuilder sql = new StringBuilder(String.format(
            "SELECT *, ST_Distance(%s, ST_SetSRID(ST_MakePoint(%f, %f), 4326)::geography) AS distance FROM %s",
            safeColumn, lon, lat, safeTable));

        if (where != null && !where.isEmpty()) sql.append(" WHERE ").append(where);
        sql.append(String.format(" ORDER BY %s <-> ST_SetSRID(ST_MakePoint(%f, %f), 4326)::geometry LIMIT %d",
            safeColumn, lon, lat, k));

        return query(sql.toString());
    }

    // ========================================================================
    // Time Travel Queries
    // ========================================================================

    /**
     * Query data as of a specific timestamp.
     *
     * @param sql SQL query
     * @param timestamp Point in time (ISO-8601 format)
     * @return Query result
     * @throws BoyoDBException if query fails
     */
    public QueryResult queryAsOf(String sql, String timestamp) throws BoyoDBException {
        String modifiedSql = sql + String.format(" AS OF TIMESTAMP '%s'", escapeString(timestamp));
        return query(modifiedSql);
    }

    /**
     * Query data changes between two timestamps.
     *
     * @param sql SQL query
     * @param startTime Start timestamp (ISO-8601 format)
     * @param endTime End timestamp (ISO-8601 format)
     * @return Query result
     * @throws BoyoDBException if query fails
     */
    public QueryResult queryBetween(String sql, String startTime, String endTime) throws BoyoDBException {
        String modifiedSql = sql + String.format(" FOR SYSTEM_TIME FROM '%s' TO '%s'",
            escapeString(startTime), escapeString(endTime));
        return query(modifiedSql);
    }

    // ========================================================================
    // Batch Operations
    // ========================================================================

    /**
     * Insert multiple rows in batches.
     *
     * @param table Table name
     * @param rows Rows to insert
     * @param chunkSize Batch size
     * @return Total rows inserted
     * @throws BoyoDBException if insert fails
     */
    public int batchInsert(String table, List<Map<String, Object>> rows, int chunkSize) throws BoyoDBException {
        if (rows == null || rows.isEmpty()) return 0;
        if (chunkSize <= 0) chunkSize = 1000;

        String safeTable = escapeIdentifier(table);
        List<String> columns = new ArrayList<>(rows.get(0).keySet());
        String colList = columns.stream().map(this::escapeIdentifier).reduce((a, b) -> a + ", " + b).orElse("");

        int totalInserted = 0;
        for (int i = 0; i < rows.size(); i += chunkSize) {
            List<Map<String, Object>> chunk = rows.subList(i, Math.min(i + chunkSize, rows.size()));
            StringBuilder values = new StringBuilder();
            for (int j = 0; j < chunk.size(); j++) {
                if (j > 0) values.append(", ");
                Map<String, Object> row = chunk.get(j);
                values.append("(");
                for (int k = 0; k < columns.size(); k++) {
                    if (k > 0) values.append(", ");
                    values.append(formatValue(row.get(columns.get(k))));
                }
                values.append(")");
            }
            exec(String.format("INSERT INTO %s (%s) VALUES %s", safeTable, colList, values));
            totalInserted += chunk.size();
        }
        return totalInserted;
    }

    /**
     * Upsert multiple rows (INSERT ... ON CONFLICT).
     *
     * @param table Table name
     * @param rows Rows to upsert
     * @param conflictColumns Columns that define the conflict
     * @param updateColumns Columns to update on conflict (null = all non-conflict columns)
     * @param chunkSize Batch size
     * @return Total rows affected
     * @throws BoyoDBException if upsert fails
     */
    public int batchUpsert(String table, List<Map<String, Object>> rows,
                            List<String> conflictColumns, List<String> updateColumns, int chunkSize) throws BoyoDBException {
        if (rows == null || rows.isEmpty()) return 0;
        if (chunkSize <= 0) chunkSize = 1000;

        String safeTable = escapeIdentifier(table);
        List<String> columns = new ArrayList<>(rows.get(0).keySet());
        String colList = columns.stream().map(this::escapeIdentifier).reduce((a, b) -> a + ", " + b).orElse("");
        String conflictClause = conflictColumns.stream().map(this::escapeIdentifier).reduce((a, b) -> a + ", " + b).orElse("");

        List<String> updateCols = updateColumns;
        if (updateCols == null || updateCols.isEmpty()) {
            Set<String> conflictSet = new HashSet<>(conflictColumns);
            updateCols = columns.stream().filter(c -> !conflictSet.contains(c)).toList();
        }
        String updateClause = updateCols.stream()
            .map(c -> escapeIdentifier(c) + " = EXCLUDED." + escapeIdentifier(c))
            .reduce((a, b) -> a + ", " + b).orElse("");

        int totalAffected = 0;
        for (int i = 0; i < rows.size(); i += chunkSize) {
            List<Map<String, Object>> chunk = rows.subList(i, Math.min(i + chunkSize, rows.size()));
            StringBuilder values = new StringBuilder();
            for (int j = 0; j < chunk.size(); j++) {
                if (j > 0) values.append(", ");
                Map<String, Object> row = chunk.get(j);
                values.append("(");
                for (int k = 0; k < columns.size(); k++) {
                    if (k > 0) values.append(", ");
                    values.append(formatValue(row.get(columns.get(k))));
                }
                values.append(")");
            }

            StringBuilder sql = new StringBuilder(String.format(
                "INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s)",
                safeTable, colList, values, conflictClause));

            if (!updateClause.isEmpty()) {
                sql.append(" DO UPDATE SET ").append(updateClause);
            } else {
                sql.append(" DO NOTHING");
            }

            exec(sql.toString());
            totalAffected += chunk.size();
        }
        return totalAffected;
    }

    /**
     * Delete multiple rows by key values.
     *
     * @param table Table name
     * @param keys Key values to delete
     * @param keyColumn Key column name
     * @param chunkSize Batch size
     * @return Total rows deleted
     * @throws BoyoDBException if delete fails
     */
    public int batchDelete(String table, List<?> keys, String keyColumn, int chunkSize) throws BoyoDBException {
        if (keys == null || keys.isEmpty()) return 0;
        if (chunkSize <= 0) chunkSize = 1000;

        String safeTable = escapeIdentifier(table);
        String safeKeyColumn = escapeIdentifier(keyColumn);

        int totalDeleted = 0;
        for (int i = 0; i < keys.size(); i += chunkSize) {
            List<?> chunk = keys.subList(i, Math.min(i + chunkSize, keys.size()));
            String values = chunk.stream().map(this::formatValue).reduce((a, b) -> a + ", " + b).orElse("");
            exec(String.format("DELETE FROM %s WHERE %s IN (%s)", safeTable, safeKeyColumn, values));
            totalDeleted += chunk.size();
        }
        return totalDeleted;
    }

    // ========================================================================
    // Schema Introspection
    // ========================================================================

    /**
     * Get column information for a table.
     *
     * @param table Table name
     * @param database Database name (optional)
     * @return Query result with column info
     * @throws BoyoDBException if query fails
     */
    public QueryResult getColumns(String table, String database) throws BoyoDBException {
        String safeTable = escapeIdentifier(table);
        String sql;
        if (database != null && !database.isEmpty()) {
            sql = "DESCRIBE " + escapeIdentifier(database) + "." + safeTable;
        } else {
            sql = "DESCRIBE " + safeTable;
        }
        return query(sql);
    }

    /**
     * Get indexes for a table.
     *
     * @param table Table name
     * @param database Database name (optional)
     * @return Query result with index info
     * @throws BoyoDBException if query fails
     */
    public QueryResult getIndexes(String table, String database) throws BoyoDBException {
        String safeTable = escapeIdentifier(table);
        String sql;
        if (database != null && !database.isEmpty()) {
            sql = "SHOW INDEXES ON " + escapeIdentifier(database) + "." + safeTable;
        } else {
            sql = "SHOW INDEXES ON " + safeTable;
        }
        return query(sql);
    }

    /**
     * Get constraints for a table.
     *
     * @param table Table name
     * @param database Database name (optional)
     * @return Query result with constraint info
     * @throws BoyoDBException if query fails
     */
    public QueryResult getConstraints(String table, String database) throws BoyoDBException {
        String safeTable = escapeIdentifier(table);
        String sql;
        if (database != null && !database.isEmpty()) {
            sql = "SHOW CONSTRAINTS ON " + escapeIdentifier(database) + "." + safeTable;
        } else {
            sql = "SHOW CONSTRAINTS ON " + safeTable;
        }
        return query(sql);
    }

    // ========================================================================
    // Array Operations
    // ========================================================================

    /**
     * Find rows where array column contains a value.
     *
     * @param table Table name
     * @param arrayColumn Array column name
     * @param value Value to search for
     * @param limit Maximum results
     * @param where Additional WHERE clause
     * @return Query result with matches
     * @throws BoyoDBException if query fails
     */
    public QueryResult arrayContains(String table, String arrayColumn, Object value,
                                      int limit, String where) throws BoyoDBException {
        if (limit <= 0) limit = 100;

        StringBuilder sql = new StringBuilder(String.format(
            "SELECT * FROM %s WHERE %s = ANY(%s)",
            escapeIdentifier(table), formatValue(value), escapeIdentifier(arrayColumn)));

        if (where != null && !where.isEmpty()) sql.append(" AND ").append(where);
        sql.append(" LIMIT ").append(limit);

        return query(sql.toString());
    }

    /**
     * Find rows where array columns have overlapping elements.
     *
     * @param table Table name
     * @param arrayColumn Array column name
     * @param values Values to check for overlap
     * @param limit Maximum results
     * @param where Additional WHERE clause
     * @return Query result with matches
     * @throws BoyoDBException if query fails
     */
    public QueryResult arrayOverlap(String table, String arrayColumn, List<?> values,
                                     int limit, String where) throws BoyoDBException {
        if (limit <= 0) limit = 100;

        String arrayLiteral = "ARRAY[" + values.stream().map(this::formatValue).reduce((a, b) -> a + ", " + b).orElse("") + "]";

        StringBuilder sql = new StringBuilder(String.format(
            "SELECT * FROM %s WHERE %s && %s",
            escapeIdentifier(table), escapeIdentifier(arrayColumn), arrayLiteral));

        if (where != null && !where.isEmpty()) sql.append(" AND ").append(where);
        sql.append(" LIMIT ").append(limit);

        return query(sql.toString());
    }

    // ========================================================================
    // Async Pub/Sub Polling
    // ========================================================================

    /**
     * Notification from pub/sub.
     */
    public static class Notification {
        public final String channel;
        public final String payload;

        public Notification(String channel, String payload) {
            this.channel = channel;
            this.payload = payload;
        }
    }

    /**
     * Poll for pending notifications.
     *
     * @param timeoutMs Timeout in milliseconds (0 = non-blocking)
     * @return List of notifications
     * @throws BoyoDBException if query fails
     */
    public List<Notification> pollNotifications(int timeoutMs) throws BoyoDBException {
        QueryResult result = query(String.format("SELECT * FROM pg_notification_queue(%d)", timeoutMs));
        List<Notification> notifications = new ArrayList<>();
        for (Map<String, Object> row : result.getRows()) {
            String channel = (String) row.get("channel");
            String payload = (String) row.get("payload");
            notifications.add(new Notification(channel, payload));
        }
        return notifications;
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
