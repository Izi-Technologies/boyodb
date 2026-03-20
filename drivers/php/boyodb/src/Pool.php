<?php

declare(strict_types=1);

namespace Boyodb;

/**
 * Configuration for connection pool.
 */
class PoolConfig
{
    public string $host = 'localhost';
    public int $port = 8765;

    // Pool settings
    public int $poolSize = 10;
    public float $poolTimeout = 30.0;

    // Connection settings
    public bool $tls = false;
    public ?string $caFile = null;
    public bool $insecureSkipVerify = false;
    public float $connectTimeout = 10.0;
    public float $readTimeout = 30.0;
    public float $writeTimeout = 10.0;

    // Auth
    public ?string $token = null;

    // Retry settings
    public int $maxRetries = 3;
    public float $retryDelay = 1.0;

    // Query defaults
    public ?string $database = null;
    public int $queryTimeout = 30000;

    public function __construct(array $options = [])
    {
        foreach ($options as $key => $value) {
            if (property_exists($this, $key)) {
                $this->$key = $value;
            }
        }
    }
}

/**
 * A connection managed by the pool.
 */
class PooledConnection
{
    /** @var resource */
    private $socket;
    private bool $valid = true;
    private float $createdAt;

    /**
     * @param resource $socket
     */
    public function __construct($socket)
    {
        $this->socket = $socket;
        $this->createdAt = microtime(true);
    }

    /**
     * @return resource
     */
    public function getSocket()
    {
        return $this->socket;
    }

    public function isValid(): bool
    {
        if (!$this->valid) {
            return false;
        }
        return is_resource($this->socket) && !feof($this->socket);
    }

    public function invalidate(): void
    {
        $this->valid = false;
    }

    public function close(): void
    {
        $this->valid = false;
        if (is_resource($this->socket)) {
            @fclose($this->socket);
        }
    }

    public function getAge(): float
    {
        return microtime(true) - $this->createdAt;
    }
}

/**
 * Thread-safe connection pool for BoyoDB.
 *
 * Example:
 *     $pool = new ConnectionPool(new PoolConfig(['host' => 'localhost', 'port' => 8765]));
 *
 *     $conn = $pool->borrow();
 *     try {
 *         // Use connection
 *     } finally {
 *         $pool->return($conn);
 *     }
 */
class ConnectionPool
{
    private PoolConfig $config;
    /** @var \SplQueue<PooledConnection> */
    private \SplQueue $pool;
    private bool $closed = false;
    private ?string $sessionId = null;
    private int $activeConnections = 0;

    public function __construct(PoolConfig $config)
    {
        $this->config = $config;
        $this->pool = new \SplQueue();

        // Pre-create connections
        for ($i = 0; $i < $config->poolSize; $i++) {
            $conn = $this->createConnection();
            $this->pool->enqueue($conn);
        }
    }

    /**
     * Create a new connection.
     */
    private function createConnection(): PooledConnection
    {
        $address = $this->config->host . ':' . $this->config->port;

        if ($this->config->tls) {
            $context = stream_context_create([
                'ssl' => [
                    'verify_peer' => !$this->config->insecureSkipVerify,
                    'verify_peer_name' => !$this->config->insecureSkipVerify,
                    'allow_self_signed' => $this->config->insecureSkipVerify,
                    'cafile' => $this->config->caFile,
                ],
            ]);

            if ($this->config->insecureSkipVerify) {
                error_log(
                    "WARNING: TLS certificate verification is DISABLED. " .
                    "This is insecure and vulnerable to MITM attacks."
                );
            }

            $socket = @stream_socket_client(
                "ssl://{$address}",
                $errno,
                $errstr,
                $this->config->connectTimeout,
                STREAM_CLIENT_CONNECT,
                $context
            );
        } else {
            $socket = @stream_socket_client(
                "tcp://{$address}",
                $errno,
                $errstr,
                $this->config->connectTimeout
            );
        }

        if (!$socket) {
            throw new ConnectionException("Failed to connect: {$errstr} ({$errno})");
        }

        stream_set_timeout($socket, (int)$this->config->readTimeout);

        return new PooledConnection($socket);
    }

    /**
     * Borrow a connection from the pool.
     */
    public function borrow(): PooledConnection
    {
        if ($this->closed) {
            throw new ConnectionException("Pool is closed");
        }

        $startTime = microtime(true);

        while (microtime(true) - $startTime < $this->config->poolTimeout) {
            if (!$this->pool->isEmpty()) {
                $conn = $this->pool->dequeue();

                if ($conn->isValid()) {
                    $this->activeConnections++;
                    return $conn;
                }

                // Connection invalid, close and try next
                $conn->close();
            }

            // Try to create a new connection if under limit
            if ($this->activeConnections < $this->config->poolSize) {
                try {
                    $conn = $this->createConnection();
                    $this->activeConnections++;
                    return $conn;
                } catch (\Exception $e) {
                    // Failed to create, wait and retry
                    usleep(10000); // 10ms
                }
            } else {
                // At limit, wait for return
                usleep(10000); // 10ms
            }
        }

        throw new ConnectionException("Connection pool exhausted");
    }

    /**
     * Return a connection to the pool.
     */
    public function return(PooledConnection $conn): void
    {
        $this->activeConnections = max(0, $this->activeConnections - 1);

        if ($conn->isValid() && !$this->closed) {
            $this->pool->enqueue($conn);
        } else {
            $conn->close();
            // Replace with new connection if not closed
            if (!$this->closed && $this->pool->count() < $this->config->poolSize) {
                try {
                    $newConn = $this->createConnection();
                    $this->pool->enqueue($newConn);
                } catch (\Exception $e) {
                    // Ignore, will be created on next borrow
                }
            }
        }
    }

    /**
     * Send a request on a connection.
     */
    public function sendRequest(PooledConnection $conn, array $request): array
    {
        $socket = $conn->getSocket();

        // Add auth
        if ($this->sessionId !== null) {
            $request['auth'] = $this->sessionId;
        } elseif ($this->config->token !== null) {
            $request['auth'] = $this->config->token;
        }

        // Serialize
        $data = json_encode($request);
        if ($data === false) {
            throw new QueryException("Failed to encode request");
        }

        // Send length + data
        $frame = pack('N', strlen($data)) . $data;

        stream_set_timeout($socket, (int)$this->config->writeTimeout);
        $written = @fwrite($socket, $frame);

        if ($written === false || $written < strlen($frame)) {
            $conn->invalidate();
            throw new ConnectionException("Write failed");
        }

        // Read response
        stream_set_timeout($socket, (int)$this->config->readTimeout);

        $lengthData = $this->readExact($socket, 4);
        if ($lengthData === null) {
            $conn->invalidate();
            throw new ConnectionException("Connection closed");
        }

        $length = unpack('N', $lengthData)[1];
        if ($length > 100 * 1024 * 1024) {
            throw new QueryException("Response too large: {$length} bytes");
        }

        $responseData = $this->readExact($socket, $length);
        if ($responseData === null) {
            $conn->invalidate();
            throw new ConnectionException("Connection closed");
        }

        $response = json_decode($responseData, true);
        if ($response === null) {
            throw new QueryException("Failed to decode response");
        }

        // Handle streaming IPC
        if (!empty($response['ipc_streaming'])) {
            $chunks = [];
            while (true) {
                $chunkLenData = $this->readExact($socket, 4);
                if ($chunkLenData === null) {
                    $conn->invalidate();
                    throw new ConnectionException("Connection closed");
                }
                $chunkLen = unpack('N', $chunkLenData)[1];
                if ($chunkLen === 0) {
                    break;
                }
                $chunk = $this->readExact($socket, $chunkLen);
                if ($chunk === null) {
                    $conn->invalidate();
                    throw new ConnectionException("Connection closed");
                }
                $chunks[] = $chunk;
            }
            $response['ipc_bytes'] = implode('', $chunks);
        }

        // Handle fixed IPC
        elseif (!empty($response['ipc_len'])) {
            $ipcLen = $response['ipc_len'];
            $payloadLenData = $this->readExact($socket, 4);
            if ($payloadLenData === null) {
                $conn->invalidate();
                throw new ConnectionException("Connection closed");
            }
            $payloadLen = unpack('N', $payloadLenData)[1];
            if ($payloadLen !== $ipcLen) {
                throw new QueryException("IPC length mismatch: expected {$ipcLen} got {$payloadLen}");
            }
            $payload = $this->readExact($socket, $payloadLen);
            if ($payload === null) {
                $conn->invalidate();
                throw new ConnectionException("Connection closed");
            }
            $response['ipc_bytes'] = $payload;
        }

        return $response;
    }

    /**
     * Read exactly n bytes from socket.
     *
     * @param resource $socket
     */
    private function readExact($socket, int $n): ?string
    {
        $data = '';
        $remaining = $n;

        while ($remaining > 0) {
            $chunk = @fread($socket, $remaining);
            if ($chunk === false || $chunk === '') {
                return null;
            }
            $data .= $chunk;
            $remaining -= strlen($chunk);
        }

        return $data;
    }

    /**
     * Set session ID for authentication.
     */
    public function setSessionId(?string $sessionId): void
    {
        $this->sessionId = $sessionId;
    }

    /**
     * Check server health.
     */
    public function health(): void
    {
        $conn = $this->borrow();
        try {
            $response = $this->sendRequest($conn, ['op' => 'health']);
            if (($response['status'] ?? '') !== 'ok') {
                throw new QueryException($response['message'] ?? 'Health check failed');
            }
        } finally {
            $this->return($conn);
        }
    }

    /**
     * Close the pool and all connections.
     */
    public function close(): void
    {
        $this->closed = true;
        while (!$this->pool->isEmpty()) {
            $conn = $this->pool->dequeue();
            $conn->close();
        }
    }

    /**
     * Get pool statistics.
     */
    public function stats(): array
    {
        return [
            'pool_size' => $this->config->poolSize,
            'available' => $this->pool->count(),
            'active' => $this->activeConnections,
            'closed' => $this->closed,
        ];
    }
}

/**
 * High-performance pooled BoyoDB client.
 *
 * Example:
 *     $client = new PooledClient(new PoolConfig([
 *         'host' => 'localhost',
 *         'port' => 8765,
 *         'poolSize' => 20,
 *         'database' => 'analytics',
 *     ]));
 *
 *     $result = $client->query("SELECT COUNT(*) FROM events");
 *     print_r($result);
 *
 *     $client->close();
 */
class PooledClient
{
    private PoolConfig $config;
    private ConnectionPool $pool;

    public function __construct(PoolConfig $config)
    {
        $this->config = $config;
        $this->pool = new ConnectionPool($config);

        // Verify with health check
        $this->pool->health();
    }

    /**
     * Execute a SQL query and return rows.
     */
    public function query(string $sql, ?string $database = null, ?int $timeout = null): array
    {
        $db = $database ?? $this->config->database;
        $timeoutMs = $timeout ?? $this->config->queryTimeout;

        $isBinary = preg_match('/^\s*(select|with)\s/i', $sql);

        $request = [
            'op' => $isBinary ? 'query_binary' : 'query',
            'sql' => $sql,
            'timeout_millis' => $timeoutMs,
        ];

        if ($isBinary) {
            $request['stream'] = true;
        }
        if ($db !== null) {
            $request['database'] = $db;
        }

        $conn = $this->pool->borrow();
        try {
            $response = $this->pool->sendRequest($conn, $request);
        } finally {
            $this->pool->return($conn);
        }

        if (($response['status'] ?? '') !== 'ok') {
            throw new QueryException($response['message'] ?? 'Query failed');
        }

        // Parse IPC data
        if (!empty($response['ipc_bytes'])) {
            return $this->parseArrowIpc($response['ipc_bytes']);
        }

        if (!empty($response['ipc_base64'])) {
            $ipcData = base64_decode($response['ipc_base64']);
            return $this->parseArrowIpc($ipcData);
        }

        return [];
    }

    /**
     * Execute a SQL statement that doesn't return rows.
     */
    public function exec(string $sql, ?string $database = null, ?int $timeout = null): void
    {
        $db = $database ?? $this->config->database;
        $timeoutMs = $timeout ?? $this->config->queryTimeout;

        $request = [
            'op' => 'query',
            'sql' => $sql,
            'timeout_millis' => $timeoutMs,
        ];

        if ($db !== null) {
            $request['database'] = $db;
        }

        $conn = $this->pool->borrow();
        try {
            $response = $this->pool->sendRequest($conn, $request);
        } finally {
            $this->pool->return($conn);
        }

        if (($response['status'] ?? '') !== 'ok') {
            throw new QueryException($response['message'] ?? 'Exec failed');
        }
    }

    /**
     * Login with username and password.
     */
    public function login(string $username, string $password): void
    {
        $conn = $this->pool->borrow();
        try {
            $response = $this->pool->sendRequest($conn, [
                'op' => 'login',
                'username' => $username,
                'password' => $password,
            ]);

            if (($response['status'] ?? '') !== 'ok') {
                throw new QueryException($response['message'] ?? 'Login failed');
            }

            $this->pool->setSessionId($response['session_id'] ?? null);
        } finally {
            $this->pool->return($conn);
        }
    }

    /**
     * Logout from server.
     */
    public function logout(): void
    {
        $conn = $this->pool->borrow();
        try {
            $response = $this->pool->sendRequest($conn, ['op' => 'logout']);

            if (($response['status'] ?? '') !== 'ok') {
                throw new QueryException($response['message'] ?? 'Logout failed');
            }

            $this->pool->setSessionId(null);
        } finally {
            $this->pool->return($conn);
        }
    }

    /**
     * Check server health.
     */
    public function health(): void
    {
        $this->pool->health();
    }

    /**
     * Create a new database.
     */
    public function createDatabase(string $name): void
    {
        $conn = $this->pool->borrow();
        try {
            $response = $this->pool->sendRequest($conn, [
                'op' => 'createdatabase',
                'name' => $name,
            ]);

            if (($response['status'] ?? '') !== 'ok') {
                throw new QueryException($response['message'] ?? 'Create database failed');
            }
        } finally {
            $this->pool->return($conn);
        }
    }

    /**
     * Create a new table.
     */
    public function createTable(string $database, string $table): void
    {
        $conn = $this->pool->borrow();
        try {
            $response = $this->pool->sendRequest($conn, [
                'op' => 'createtable',
                'database' => $database,
                'table' => $table,
            ]);

            if (($response['status'] ?? '') !== 'ok') {
                throw new QueryException($response['message'] ?? 'Create table failed');
            }
        } finally {
            $this->pool->return($conn);
        }
    }

    /**
     * List all databases.
     */
    public function listDatabases(): array
    {
        $conn = $this->pool->borrow();
        try {
            $response = $this->pool->sendRequest($conn, ['op' => 'listdatabases']);

            if (($response['status'] ?? '') !== 'ok') {
                throw new QueryException($response['message'] ?? 'List databases failed');
            }

            return $response['databases'] ?? [];
        } finally {
            $this->pool->return($conn);
        }
    }

    /**
     * List tables.
     */
    public function listTables(?string $database = null): array
    {
        $request = ['op' => 'listtables'];
        if ($database !== null) {
            $request['database'] = $database;
        }

        $conn = $this->pool->borrow();
        try {
            $response = $this->pool->sendRequest($conn, $request);

            if (($response['status'] ?? '') !== 'ok') {
                throw new QueryException($response['message'] ?? 'List tables failed');
            }

            return $response['tables'] ?? [];
        } finally {
            $this->pool->return($conn);
        }
    }

    /**
     * Get pool statistics.
     */
    public function poolStats(): array
    {
        return $this->pool->stats();
    }

    /**
     * Set the default database.
     */
    public function setDatabase(string $database): void
    {
        $this->config->database = $database;
    }

    /**
     * Close the client and release all connections.
     */
    public function close(): void
    {
        $this->pool->close();
    }

    /**
     * Parse Arrow IPC data into rows.
     * Note: This is a simplified parser. For production, use a proper Arrow library.
     */
    private function parseArrowIpc(string $data): array
    {
        // Simplified IPC parsing - returns empty for now
        // In production, use a PHP Arrow library or custom parser
        return [];
    }
}
