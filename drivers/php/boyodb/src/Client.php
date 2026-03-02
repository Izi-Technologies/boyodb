<?php

declare(strict_types=1);

namespace Boyodb;

/**
 * boyodb client for PHP.
 */
class Client
{
    private string $hostname;
    private int $port;
    private Config $config;
    /** @var resource|null */
    private $socket = null;
    private ?string $sessionId = null;

    /**
     * Create a new boyodb client.
     *
     * @param string $host Server address in host:port format
     * @param Config|null $config Configuration options
     */
    public function __construct(string $host, ?Config $config = null)
    {
        $parts = explode(':', $host);
        $this->hostname = $parts[0] ?: 'localhost';
        $this->port = isset($parts[1]) ? (int)$parts[1] : 8765;
        $this->config = $config ?? new Config();
    }

    /**
     * Connect to the server.
     *
     * @throws ConnectionException
     */
    public function connect(): void
    {
        $this->connectWithRetry();

        try {
            $this->health();
        } catch (\Exception $e) {
            $this->close();
            throw new ConnectionException("Health check failed: " . $e->getMessage());
        }
    }

    /**
     * Connect with retry logic.
     */
    private function connectWithRetry(): void
    {
        $lastError = null;

        for ($attempt = 0; $attempt < $this->config->maxRetries; $attempt++) {
            try {
                $this->connectOnce();
                return;
            } catch (\Exception $e) {
                $lastError = $e;
                if ($attempt < $this->config->maxRetries - 1) {
                    usleep((int)($this->config->retryDelay * 1000000));
                }
            }
        }

        throw new ConnectionException(
            "Failed to connect after {$this->config->maxRetries} attempts: " .
            ($lastError ? $lastError->getMessage() : 'Unknown error')
        );
    }

    /**
     * Single connection attempt.
     */
    private function connectOnce(): void
    {
        $address = $this->hostname . ':' . $this->port;

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
                    "This is insecure and vulnerable to MITM attacks. " .
                    "Only use for testing with self-signed certificates."
                );
            }

            $socket = @stream_socket_client(
                'ssl://' . $address,
                $errno,
                $errstr,
                $this->config->connectTimeout,
                STREAM_CLIENT_CONNECT,
                $context
            );
        } else {
            $socket = @stream_socket_client(
                'tcp://' . $address,
                $errno,
                $errstr,
                $this->config->connectTimeout
            );
        }

        if ($socket === false) {
            throw new ConnectionException("Connection failed: $errstr ($errno)");
        }

        stream_set_timeout($socket, (int)$this->config->readTimeout);
        $this->socket = $socket;
    }

    /**
     * Close the connection.
     */
    public function close(): void
    {
        if ($this->socket !== null) {
            fclose($this->socket);
            $this->socket = null;
        }
    }

    /**
     * Destructor - close connection.
     */
    public function __destruct()
    {
        $this->close();
    }

    /**
     * Send a request to the server.
     *
     * @param array<string, mixed> $request
     * @return array<string, mixed>
     * @throws ConnectionException|QueryException
     */
    private function sendRequest(array $request): array
    {
        if ($this->socket === null) {
            throw new ConnectionException("Not connected");
        }

        // Add auth if available
        if ($this->sessionId !== null) {
            $request['auth'] = $this->sessionId;
        } elseif ($this->config->token !== null) {
            $request['auth'] = $this->config->token;
        }

        // Serialize request
        $json = json_encode($request);
        if ($json === false) {
            throw new QueryException("Failed to encode request");
        }

        // Write length prefix (big-endian)
        $length = strlen($json);
        $lengthBytes = pack('N', $length);

        if (fwrite($this->socket, $lengthBytes . $json) === false) {
            throw new ConnectionException("Failed to write request");
        }
        fflush($this->socket);

        // Read response length
        $lengthData = $this->readExact(4);
        $unpacked = unpack('N', $lengthData);
        if ($unpacked === false) {
            throw new ConnectionException("Failed to read response length");
        }
        $respLength = $unpacked[1];

        if ($respLength > 100 * 1024 * 1024) {
            throw new QueryException("Response too large: $respLength bytes");
        }

        // Read response body
        $respData = $this->readExact($respLength);
        $response = json_decode($respData, true);

        if ($response === null) {
            throw new QueryException("Failed to decode response");
        }

        if (!empty($response['ipc_streaming'])) {
            $response['ipc_bytes'] = $this->readStreamFrames();
        } elseif (isset($response['ipc_len']) && is_int($response['ipc_len']) && $response['ipc_len'] > 0) {
            $ipcLen = $response['ipc_len'];
            if ($ipcLen > 100 * 1024 * 1024) {
                throw new QueryException("Response too large: $ipcLen bytes");
            }
            $payloadLenData = $this->readExact(4);
            $payloadUnpacked = unpack('N', $payloadLenData);
            if ($payloadUnpacked === false) {
                throw new ConnectionException("Failed to read IPC length");
            }
            $payloadLen = $payloadUnpacked[1];
            if ($payloadLen !== $ipcLen) {
                throw new QueryException("IPC length mismatch: expected $ipcLen got $payloadLen");
            }
            $response['ipc_bytes'] = $this->readExact($payloadLen);
        }

        return $response;
    }

    /**
     * Read exactly n bytes from socket.
     */
    private function readExact(int $length): string
    {
        $data = '';
        $remaining = $length;

        while ($remaining > 0) {
            $chunk = fread($this->socket, $remaining);
            if ($chunk === false || $chunk === '') {
                throw new ConnectionException("Connection closed");
            }
            $data .= $chunk;
            $remaining -= strlen($chunk);
        }

        return $data;
    }

    private function readStreamFrames(): string
    {
        $payload = '';
        while (true) {
            $lengthData = $this->readExact(4);
            $unpacked = unpack('N', $lengthData);
            if ($unpacked === false) {
                throw new ConnectionException("Failed to read IPC length");
            }
            $chunkLen = $unpacked[1];
            if ($chunkLen === 0) {
                break;
            }
            if ($chunkLen > 100 * 1024 * 1024) {
                throw new QueryException("Response too large: $chunkLen bytes");
            }
            $payload .= $this->readExact($chunkLen);
        }
        return $payload;
    }

    /**
     * Check response status.
     *
     * @throws QueryException
     */
    private function checkStatus(array $response, string $errorPrefix): void
    {
        if (($response['status'] ?? '') !== 'ok') {
            $message = $response['message'] ?? 'Unknown error';
            throw new QueryException("$errorPrefix: $message");
        }
    }

    /**
     * Check server health.
     *
     * @throws QueryException
     */
    public function health(): void
    {
        $response = $this->sendRequest(['op' => 'health']);
        $this->checkStatus($response, 'Health check failed');
    }

    /**
     * Login with username and password.
     *
     * @throws AuthException
     */
    public function login(string $username, string $password): void
    {
        $response = $this->sendRequest([
            'op' => 'login',
            'username' => $username,
            'password' => $password,
        ]);

        if (($response['status'] ?? '') !== 'ok') {
            throw new AuthException($response['message'] ?? 'Login failed');
        }

        $this->sessionId = $response['session_id'] ?? null;
    }

    /**
     * Logout from the server.
     *
     * @throws AuthException
     */
    public function logout(): void
    {
        $response = $this->sendRequest(['op' => 'logout']);

        if (($response['status'] ?? '') !== 'ok') {
            throw new AuthException($response['message'] ?? 'Logout failed');
        }

        $this->sessionId = null;
    }

    /**
     * Execute a SQL query.
     *
     * @throws QueryException
     */
    public function query(string $sql, ?string $database = null, ?int $timeout = null): QueryResult
    {
        $isBinary = $this->isSelectLike($sql);
        $request = [
            'op' => $isBinary ? 'query_binary' : 'query',
            'sql' => $sql,
            'timeout_millis' => $timeout ?? $this->config->queryTimeout,
        ];
        if ($isBinary) {
            $request['stream'] = true;
        }

        $db = $database ?? $this->config->database;
        if ($db !== null) {
            $request['database'] = $db;
        }

        $response = $this->sendRequest($request);
        $this->checkStatus($response, 'Query failed');

        $result = new QueryResult();
        $result->segmentsScanned = $response['segments_scanned'] ?? 0;
        $result->dataSkippedBytes = $response['data_skipped_bytes'] ?? 0;

        if (isset($response['ipc_bytes']) && is_string($response['ipc_bytes'])) {
            $this->parseArrowIpc($response['ipc_bytes'], $result);
        } elseif (isset($response['ipc_base64'])) {
            $ipcData = base64_decode($response['ipc_base64']);
            if ($ipcData !== false) {
                $this->parseArrowIpc($ipcData, $result);
            }
        }

        return $result;
    }

    /**
     * Execute a SQL statement that doesn't return rows.
     *
     * @throws QueryException
     */
    public function exec(string $sql, ?string $database = null, ?int $timeout = null): void
    {
        $this->query($sql, $database, $timeout);
    }

    /**
     * Prepare a SELECT query on the server and return a prepared id.
     *
     * @throws QueryException
     */
    public function prepare(string $sql, ?string $database = null): string
    {
        $request = [
            'op' => 'prepare',
            'sql' => $sql,
        ];

        $db = $database ?? $this->config->database;
        if ($db !== null) {
            $request['database'] = $db;
        }

        $response = $this->sendRequest($request);
        $this->checkStatus($response, 'Prepare failed');

        $preparedId = $response['prepared_id'] ?? null;
        if (!is_string($preparedId) || $preparedId === '') {
            throw new QueryException('missing prepared_id in response');
        }

        return $preparedId;
    }

    /**
     * Execute a prepared statement using binary IPC responses.
     *
     * @throws QueryException
     */
    public function executePreparedBinary(string $preparedId, ?int $timeout = null): QueryResult
    {
        $request = [
            'op' => 'execute_prepared_binary',
            'id' => $preparedId,
            'timeout_millis' => $timeout ?? $this->config->queryTimeout,
        ];
        $request['stream'] = true;

        $response = $this->sendRequest($request);
        $this->checkStatus($response, 'Execute prepared failed');

        $result = new QueryResult();
        $result->segmentsScanned = $response['segments_scanned'] ?? 0;
        $result->dataSkippedBytes = $response['data_skipped_bytes'] ?? 0;

        if (isset($response['ipc_bytes']) && is_string($response['ipc_bytes'])) {
            $this->parseArrowIpc($response['ipc_bytes'], $result);
        } elseif (isset($response['ipc_base64'])) {
            $ipcData = base64_decode($response['ipc_base64']);
            if ($ipcData !== false) {
                $this->parseArrowIpc($ipcData, $result);
            }
        }

        return $result;
    }

    private function isSelectLike(string $sql): bool
    {
        $trimmed = ltrim($sql);
        $lower = strtolower($trimmed);
        return str_starts_with($lower, 'select ') || str_starts_with($lower, 'with ');
    }

    /**
     * Create a new database.
     *
     * @throws QueryException
     */
    public function createDatabase(string $name): void
    {
        $response = $this->sendRequest([
            'op' => 'createdatabase',
            'name' => $name,
        ]);
        $this->checkStatus($response, 'Create database failed');
    }

    /**
     * Create a new table.
     *
     * @throws QueryException
     */
    public function createTable(string $database, string $table): void
    {
        $response = $this->sendRequest([
            'op' => 'createtable',
            'database' => $database,
            'table' => $table,
        ]);
        $this->checkStatus($response, 'Create table failed');
    }

    /**
     * List all databases.
     *
     * @return array<int, string>
     * @throws QueryException
     */
    public function listDatabases(): array
    {
        $response = $this->sendRequest(['op' => 'listdatabases']);
        $this->checkStatus($response, 'List databases failed');
        return $response['databases'] ?? [];
    }

    /**
     * List tables, optionally filtered by database.
     *
     * @return array<int, TableInfo>
     * @throws QueryException
     */
    public function listTables(?string $database = null): array
    {
        $request = ['op' => 'listtables'];
        if ($database !== null) {
            $request['database'] = $database;
        }

        $response = $this->sendRequest($request);
        $this->checkStatus($response, 'List tables failed');

        $tables = [];
        foreach ($response['tables'] ?? [] as $t) {
            $tables[] = TableInfo::fromArray($t);
        }

        return $tables;
    }

    /**
     * Get query execution plan.
     *
     * @return array<string, mixed>
     * @throws QueryException
     */
    public function explain(string $sql): array
    {
        $response = $this->sendRequest([
            'op' => 'explain',
            'sql' => $sql,
        ]);
        $this->checkStatus($response, 'Explain failed');
        return $response['explain_plan'] ?? [];
    }

    /**
     * Get server metrics.
     *
     * @return array<string, mixed>
     * @throws QueryException
     */
    public function metrics(): array
    {
        $response = $this->sendRequest(['op' => 'metrics']);
        $this->checkStatus($response, 'Metrics failed');
        return $response['metrics'] ?? [];
    }

    /**
     * Ingest CSV data into a table.
     *
     * @throws QueryException
     */
    public function ingestCsv(
        string $database,
        string $table,
        string $csvData,
        bool $hasHeader = true,
        ?string $delimiter = null
    ): void {
        $request = [
            'op' => 'ingestcsv',
            'database' => $database,
            'table' => $table,
            'payload_base64' => base64_encode($csvData),
            'has_header' => $hasHeader,
        ];

        if ($delimiter !== null) {
            $request['delimiter'] = $delimiter;
        }

        $response = $this->sendRequest($request);
        $this->checkStatus($response, 'Ingest CSV failed');
    }

    /**
     * Ingest Arrow IPC data into a table.
     *
     * @throws QueryException
     */
    public function ingestIpc(string $database, string $table, string $ipcData): void
    {
        try {
            $this->sendBinaryRequest(
                [
                    'op' => 'ingest_ipc_binary',
                    'database' => $database,
                    'table' => $table,
                ],
                $ipcData
            );
            return;
        } catch (\Throwable $e) {
            // Fall back to base64 on older servers.
        }

        $response = $this->sendRequest([
            'op' => 'ingestipc',
            'database' => $database,
            'table' => $table,
            'payload_base64' => base64_encode($ipcData),
        ]);
        $this->checkStatus($response, 'Ingest IPC failed');
    }

    /**
     * Send a request with a binary payload.
     *
     * @param array<string, mixed> $request
     * @throws ConnectionException|QueryException
     */
    private function sendBinaryRequest(array $request, string $payload): void
    {
        if ($this->socket === null) {
            throw new ConnectionException("Not connected");
        }

        if ($this->sessionId !== null) {
            $request['auth'] = $this->sessionId;
        } elseif ($this->config->token !== null) {
            $request['auth'] = $this->config->token;
        }

        $json = json_encode($request);
        if ($json === false) {
            throw new QueryException("Failed to encode request");
        }

        $headerLen = strlen($json);
        $headerLenBytes = pack('N', $headerLen);
        $payloadLenBytes = pack('N', strlen($payload));

        if (fwrite($this->socket, $headerLenBytes . $json . $payloadLenBytes . $payload) === false) {
            throw new ConnectionException("Failed to write request");
        }
        fflush($this->socket);

        $lengthData = $this->readExact(4);
        $unpacked = unpack('N', $lengthData);
        if ($unpacked === false) {
            throw new ConnectionException("Failed to read response length");
        }
        $respLength = $unpacked[1];
        if ($respLength > 100 * 1024 * 1024) {
            throw new QueryException("Response too large: $respLength bytes");
        }
        $respData = $this->readExact($respLength);
        $response = json_decode($respData, true);
        if ($response === null) {
            throw new QueryException("Failed to decode response");
        }
        $this->checkStatus($response, 'Ingest IPC failed');
    }

    /**
     * Set the default database.
     */
    public function setDatabase(string $database): void
    {
        $this->config->database = $database;
    }

    /**
     * Set the authentication token.
     */
    public function setToken(string $token): void
    {
        $this->config->token = $token;
    }

    /**
     * Parse Arrow IPC data (simplified parser).
     * For production use, consider using a PHP Arrow library.
     *
     * @param string $data Binary IPC data
     * @param QueryResult $result Result object to populate
     */
    private function parseArrowIpc(string $data, QueryResult $result): void
    {
        $offset = 0;
        $length = strlen($data);

        while ($offset < $length) {
            if ($offset + 4 > $length) {
                break;
            }

            $msgLen = unpack('V', substr($data, $offset, 4))[1];
            // Convert to signed 32-bit
            if ($msgLen > 0x7FFFFFFF) {
                $msgLen = $msgLen - 0x100000000;
            }
            $offset += 4;

            if ($msgLen === 0) {
                break;
            }

            // Handle continuation marker
            if ($msgLen === -1) {
                if ($offset + 4 > $length) {
                    break;
                }
                $msgLen = unpack('V', substr($data, $offset, 4))[1];
                $offset += 4;
                if ($msgLen === 0) {
                    break;
                }
            }

            if ($offset + $msgLen > $length) {
                break;
            }

            // Skip metadata for now
            $offset += $msgLen;

            // Pad to 8-byte boundary
            $padding = (8 - ($msgLen % 8)) % 8;
            $offset += $padding;
        }

        // Return empty result - full Arrow parsing requires dedicated library
    }
}
