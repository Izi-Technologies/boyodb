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

    // Transaction Support

    /**
     * Begin a transaction.
     *
     * @param string|null $isolationLevel Optional isolation level
     * @param bool $readOnly Whether transaction is read-only
     * @throws QueryException
     */
    public function begin(?string $isolationLevel = null, bool $readOnly = false): void
    {
        if ($isolationLevel !== null) {
            $sql = "START TRANSACTION ISOLATION LEVEL $isolationLevel";
            if ($readOnly) {
                $sql .= " READ ONLY";
            }
        } elseif ($readOnly) {
            $sql = "START TRANSACTION READ ONLY";
        } else {
            $sql = "BEGIN";
        }
        $this->exec($sql);
    }

    /**
     * Commit the current transaction.
     *
     * @throws QueryException
     */
    public function commit(): void
    {
        $this->exec("COMMIT");
    }

    /**
     * Rollback the current transaction or to a savepoint.
     *
     * @param string|null $savepoint If provided, rollback to this savepoint
     * @throws QueryException
     */
    public function rollback(?string $savepoint = null): void
    {
        if ($savepoint !== null) {
            $this->exec("ROLLBACK TO SAVEPOINT $savepoint");
        } else {
            $this->exec("ROLLBACK");
        }
    }

    /**
     * Create a savepoint.
     *
     * @throws QueryException
     */
    public function savepoint(string $name): void
    {
        $this->exec("SAVEPOINT $name");
    }

    /**
     * Release a savepoint.
     *
     * @throws QueryException
     */
    public function releaseSavepoint(string $name): void
    {
        $this->exec("RELEASE SAVEPOINT $name");
    }

    // ========================================================================
    // Pub/Sub Support
    // ========================================================================

    /**
     * Start listening on a notification channel.
     *
     * @throws QueryException
     */
    public function listen(string $channel): void
    {
        $this->exec("LISTEN " . $this->escapeIdentifier($channel));
    }

    /**
     * Stop listening on a notification channel.
     *
     * @param string $channel Channel name, or "*" to unlisten all
     * @throws QueryException
     */
    public function unlisten(string $channel = '*'): void
    {
        if ($channel === '*') {
            $this->exec("UNLISTEN *");
        } else {
            $this->exec("UNLISTEN " . $this->escapeIdentifier($channel));
        }
    }

    /**
     * Send a notification on a channel.
     *
     * @param string $channel Channel name
     * @param string|null $payload Optional payload
     * @throws QueryException
     */
    public function notify(string $channel, ?string $payload = null): void
    {
        if ($payload !== null) {
            $this->exec("NOTIFY " . $this->escapeIdentifier($channel) . ", '" . $this->escapeString($payload) . "'");
        } else {
            $this->exec("NOTIFY " . $this->escapeIdentifier($channel));
        }
    }

    // ========================================================================
    // Trigger Management
    // ========================================================================

    /**
     * Create a database trigger.
     *
     * @param string $name Trigger name
     * @param string $table Table name
     * @param string $timing When to fire: "BEFORE", "AFTER", or "INSTEAD OF"
     * @param array<string> $events Events to trigger on
     * @param string $function Function to execute
     * @param array<mixed>|null $arguments Arguments to pass to function
     * @param bool $forEachRow Whether to fire for each row (vs statement)
     * @param string|null $whenClause Optional WHEN condition
     * @param bool $orReplace Whether to replace existing trigger
     * @throws QueryException
     */
    public function createTrigger(
        string $name,
        string $table,
        string $timing,
        array $events,
        string $function,
        ?array $arguments = null,
        bool $forEachRow = true,
        ?string $whenClause = null,
        bool $orReplace = false
    ): void {
        $sql = $orReplace ? "CREATE OR REPLACE TRIGGER " : "CREATE TRIGGER ";
        $sql .= $this->escapeIdentifier($name) . " ";
        $sql .= strtoupper($timing) . " ";
        $sql .= implode(" OR ", array_map('strtoupper', $events)) . " ";
        $sql .= "ON " . $this->escapeIdentifier($table) . " ";
        $sql .= $forEachRow ? "FOR EACH ROW " : "FOR EACH STATEMENT ";

        if ($whenClause !== null) {
            $sql .= "WHEN ($whenClause) ";
        }

        $sql .= "EXECUTE FUNCTION " . $this->escapeIdentifier($function) . "(";
        if ($arguments !== null && count($arguments) > 0) {
            $sql .= implode(", ", array_map([$this, 'formatValue'], $arguments));
        }
        $sql .= ")";

        $this->exec($sql);
    }

    /**
     * Drop a trigger.
     *
     * @throws QueryException
     */
    public function dropTrigger(string $name, string $table, bool $ifExists = false): void
    {
        $sql = "DROP TRIGGER ";
        if ($ifExists) {
            $sql .= "IF EXISTS ";
        }
        $sql .= $this->escapeIdentifier($name) . " ON " . $this->escapeIdentifier($table);
        $this->exec($sql);
    }

    /**
     * Enable or disable a trigger.
     *
     * @throws QueryException
     */
    public function alterTrigger(string $name, string $table, bool $enable = true): void
    {
        $action = $enable ? "ENABLE" : "DISABLE";
        $this->exec("ALTER TRIGGER " . $this->escapeIdentifier($name) . " ON " . $this->escapeIdentifier($table) . " $action");
    }

    /**
     * List triggers on a table or in a database.
     *
     * @throws QueryException
     */
    public function listTriggers(?string $table = null, ?string $database = null): QueryResult
    {
        $sql = "SHOW TRIGGERS";
        if ($table !== null) {
            $sql .= " ON " . $this->escapeIdentifier($table);
        } elseif ($database !== null) {
            $sql .= " IN " . $this->escapeIdentifier($database);
        }
        return $this->query($sql);
    }

    // ========================================================================
    // Stored Procedures and Functions
    // ========================================================================

    /**
     * Call a stored procedure.
     *
     * @param string $procedure Procedure name
     * @param mixed ...$args Arguments to pass
     * @throws QueryException
     */
    public function call(string $procedure, mixed ...$args): QueryResult
    {
        $sql = "CALL " . $this->escapeIdentifier($procedure) . "(";
        $sql .= implode(", ", array_map([$this, 'formatValue'], $args));
        $sql .= ")";
        return $this->query($sql);
    }

    /**
     * Create a stored function.
     *
     * @param string $name Function name
     * @param array<array{name: string, type: string}> $parameters Parameters
     * @param string $returnType Return type
     * @param string $body Function body
     * @param string $language Language (default: plpgsql)
     * @param bool $orReplace Whether to replace existing function
     * @param string|null $volatility IMMUTABLE, STABLE, or VOLATILE
     * @throws QueryException
     */
    public function createFunction(
        string $name,
        array $parameters,
        string $returnType,
        string $body,
        string $language = 'plpgsql',
        bool $orReplace = false,
        ?string $volatility = null
    ): void {
        $sql = $orReplace ? "CREATE OR REPLACE FUNCTION " : "CREATE FUNCTION ";
        $sql .= $this->escapeIdentifier($name) . "(";

        $params = [];
        foreach ($parameters as $param) {
            $params[] = $this->escapeIdentifier($param['name']) . " " . $param['type'];
        }
        $sql .= implode(", ", $params);

        $sql .= ") RETURNS $returnType LANGUAGE $language ";

        if ($volatility !== null) {
            $sql .= strtoupper($volatility) . " ";
        }

        $sql .= "AS \$\$ $body \$\$";

        $this->exec($sql);
    }

    /**
     * Create a stored procedure.
     *
     * @param string $name Procedure name
     * @param array<array{name: string, type: string, mode?: string}> $parameters Parameters
     * @param string $body Procedure body
     * @param string $language Language (default: plpgsql)
     * @param bool $orReplace Whether to replace existing procedure
     * @throws QueryException
     */
    public function createProcedure(
        string $name,
        array $parameters,
        string $body,
        string $language = 'plpgsql',
        bool $orReplace = false
    ): void {
        $sql = $orReplace ? "CREATE OR REPLACE PROCEDURE " : "CREATE PROCEDURE ";
        $sql .= $this->escapeIdentifier($name) . "(";

        $params = [];
        foreach ($parameters as $param) {
            $mode = isset($param['mode']) ? strtoupper($param['mode']) . " " : "";
            $params[] = $mode . $this->escapeIdentifier($param['name']) . " " . $param['type'];
        }
        $sql .= implode(", ", $params);

        $sql .= ") LANGUAGE $language AS \$\$ $body \$\$";

        $this->exec($sql);
    }

    /**
     * Drop a function.
     *
     * @param string $name Function name
     * @param array<string>|null $paramTypes Parameter types for overload resolution
     * @param bool $ifExists Don't error if function doesn't exist
     * @throws QueryException
     */
    public function dropFunction(string $name, ?array $paramTypes = null, bool $ifExists = false): void
    {
        $sql = "DROP FUNCTION ";
        if ($ifExists) {
            $sql .= "IF EXISTS ";
        }
        $sql .= $this->escapeIdentifier($name);
        if ($paramTypes !== null) {
            $sql .= "(" . implode(", ", $paramTypes) . ")";
        }
        $this->exec($sql);
    }

    /**
     * Drop a procedure.
     *
     * @param string $name Procedure name
     * @param array<string>|null $paramTypes Parameter types for overload resolution
     * @param bool $ifExists Don't error if procedure doesn't exist
     * @throws QueryException
     */
    public function dropProcedure(string $name, ?array $paramTypes = null, bool $ifExists = false): void
    {
        $sql = "DROP PROCEDURE ";
        if ($ifExists) {
            $sql .= "IF EXISTS ";
        }
        $sql .= $this->escapeIdentifier($name);
        if ($paramTypes !== null) {
            $sql .= "(" . implode(", ", $paramTypes) . ")";
        }
        $this->exec($sql);
    }

    // ========================================================================
    // JSON Operations
    // ========================================================================

    /**
     * Extract a JSON value using the -> operator.
     *
     * @throws QueryException
     */
    public function jsonExtract(
        string $column,
        string $path,
        string $table,
        ?string $where = null,
        ?int $limit = null
    ): QueryResult {
        $pathExpr = is_numeric($path) ? $path : "'" . $this->escapeString($path) . "'";
        $sql = "SELECT " . $this->escapeIdentifier($column) . " -> $pathExpr AS value FROM " . $this->escapeIdentifier($table);
        if ($where !== null) {
            $sql .= " WHERE $where";
        }
        if ($limit !== null && $limit > 0) {
            $sql .= " LIMIT $limit";
        }
        return $this->query($sql);
    }

    /**
     * Extract a JSON value as text using the ->> operator.
     *
     * @throws QueryException
     */
    public function jsonExtractText(
        string $column,
        string $path,
        string $table,
        ?string $where = null,
        ?int $limit = null
    ): QueryResult {
        $pathExpr = is_numeric($path) ? $path : "'" . $this->escapeString($path) . "'";
        $sql = "SELECT " . $this->escapeIdentifier($column) . " ->> $pathExpr AS value FROM " . $this->escapeIdentifier($table);
        if ($where !== null) {
            $sql .= " WHERE $where";
        }
        if ($limit !== null && $limit > 0) {
            $sql .= " LIMIT $limit";
        }
        return $this->query($sql);
    }

    /**
     * Extract a nested JSON value using the #> operator.
     *
     * @param array<string> $path Array of path elements
     * @throws QueryException
     */
    public function jsonExtractPath(
        string $column,
        array $path,
        string $table,
        ?string $where = null,
        ?int $limit = null
    ): QueryResult {
        $pathArray = "'{" . implode(",", array_map([$this, 'escapeString'], $path)) . "}'";
        $sql = "SELECT " . $this->escapeIdentifier($column) . " #> $pathArray AS value FROM " . $this->escapeIdentifier($table);
        if ($where !== null) {
            $sql .= " WHERE $where";
        }
        if ($limit !== null && $limit > 0) {
            $sql .= " LIMIT $limit";
        }
        return $this->query($sql);
    }

    /**
     * Check if JSON contains a value using the @> operator.
     *
     * @param mixed $value Value to check for
     * @throws QueryException
     */
    public function jsonContains(
        string $column,
        mixed $value,
        string $table,
        ?string $where = null,
        ?int $limit = null
    ): QueryResult {
        $jsonValue = json_encode($value);
        $sql = "SELECT * FROM " . $this->escapeIdentifier($table) . " WHERE " .
               $this->escapeIdentifier($column) . " @> '" . $this->escapeString($jsonValue) . "'::jsonb";
        if ($where !== null) {
            $sql .= " AND $where";
        }
        if ($limit !== null && $limit > 0) {
            $sql .= " LIMIT $limit";
        }
        return $this->query($sql);
    }

    /**
     * Check if JSON is contained in a value using the <@ operator.
     *
     * @param mixed $value Value to check against
     * @throws QueryException
     */
    public function jsonContainedBy(
        string $column,
        mixed $value,
        string $table,
        ?string $where = null,
        ?int $limit = null
    ): QueryResult {
        $jsonValue = json_encode($value);
        $sql = "SELECT * FROM " . $this->escapeIdentifier($table) . " WHERE " .
               $this->escapeIdentifier($column) . " <@ '" . $this->escapeString($jsonValue) . "'::jsonb";
        if ($where !== null) {
            $sql .= " AND $where";
        }
        if ($limit !== null && $limit > 0) {
            $sql .= " LIMIT $limit";
        }
        return $this->query($sql);
    }

    /**
     * Check if a JSON path exists using the @? operator.
     *
     * @throws QueryException
     */
    public function jsonPathExists(
        string $column,
        string $jsonPath,
        string $table,
        ?string $where = null,
        ?int $limit = null
    ): QueryResult {
        $sql = "SELECT * FROM " . $this->escapeIdentifier($table) . " WHERE " .
               $this->escapeIdentifier($column) . " @? '" . $this->escapeString($jsonPath) . "'";
        if ($where !== null) {
            $sql .= " AND $where";
        }
        if ($limit !== null && $limit > 0) {
            $sql .= " LIMIT $limit";
        }
        return $this->query($sql);
    }

    /**
     * Check if a JSON path predicate matches using the @@ operator.
     *
     * @throws QueryException
     */
    public function jsonPathMatch(
        string $column,
        string $jsonPath,
        string $table,
        ?string $where = null,
        ?int $limit = null
    ): QueryResult {
        $sql = "SELECT * FROM " . $this->escapeIdentifier($table) . " WHERE " .
               $this->escapeIdentifier($column) . " @@ '" . $this->escapeString($jsonPath) . "'";
        if ($where !== null) {
            $sql .= " AND $where";
        }
        if ($limit !== null && $limit > 0) {
            $sql .= " LIMIT $limit";
        }
        return $this->query($sql);
    }

    // ========================================================================
    // Cursor Support
    // ========================================================================

    /**
     * Declare a server-side cursor.
     *
     * @throws QueryException
     */
    public function declareCursor(string $name, string $queryStr, bool $scroll = false, bool $hold = false): void
    {
        $sql = "DECLARE " . $this->escapeIdentifier($name);
        if ($scroll) $sql .= " SCROLL";
        if ($hold) $sql .= " WITH HOLD";
        $sql .= " CURSOR FOR " . $queryStr;
        $this->exec($sql);
    }

    /**
     * Fetch rows from a cursor.
     *
     * @throws QueryException
     */
    public function fetchCursor(string $name, int $count = 1, string $direction = 'NEXT'): QueryResult
    {
        $dir = strtoupper($direction);
        $safeName = $this->escapeIdentifier($name);
        if (in_array($dir, ['NEXT', 'PRIOR', 'FIRST', 'LAST'], true)) {
            $sql = "FETCH $dir $count FROM $safeName";
        } else {
            $sql = "FETCH $dir FROM $safeName";
        }
        return $this->query($sql);
    }

    /**
     * Move cursor position without returning data.
     *
     * @throws QueryException
     */
    public function moveCursor(string $name, int $count = 1, string $direction = 'NEXT'): void
    {
        $dir = strtoupper($direction);
        $safeName = $this->escapeIdentifier($name);
        if (in_array($dir, ['NEXT', 'PRIOR', 'FIRST', 'LAST'], true)) {
            $sql = "MOVE $dir $count IN $safeName";
        } else {
            $sql = "MOVE $dir IN $safeName";
        }
        $this->exec($sql);
    }

    /**
     * Close a cursor.
     *
     * @throws QueryException
     */
    public function closeCursor(string $name): void
    {
        if ($name === '*' || strtoupper($name) === 'ALL') {
            $this->exec("CLOSE ALL");
        } else {
            $this->exec("CLOSE " . $this->escapeIdentifier($name));
        }
    }

    // ========================================================================
    // Large Objects (LOB) Support
    // ========================================================================

    /**
     * Create a new large object.
     *
     * @throws QueryException
     */
    public function loCreate(): int
    {
        $result = $this->query("SELECT lo_create(0) AS oid");
        return (int)$result->rows[0]['oid'];
    }

    /**
     * Open a large object.
     *
     * @throws QueryException
     */
    public function loOpen(int $oid, string $mode = 'rw'): int
    {
        $modeFlag = 0x40000; // INV_READ
        if (str_contains($mode, 'w')) $modeFlag |= 0x20000;
        $result = $this->query("SELECT lo_open($oid, $modeFlag) AS fd");
        return (int)$result->rows[0]['fd'];
    }

    /**
     * Write data to a large object.
     *
     * @throws QueryException
     */
    public function loWrite(int $fd, string $data): int
    {
        $b64 = base64_encode($data);
        $result = $this->query("SELECT lowrite($fd, decode('$b64', 'base64')) AS written");
        return (int)$result->rows[0]['written'];
    }

    /**
     * Read data from a large object.
     *
     * @throws QueryException
     */
    public function loRead(int $fd, int $length): string
    {
        $result = $this->query("SELECT encode(loread($fd, $length), 'base64') AS data");
        return base64_decode($result->rows[0]['data']);
    }

    /**
     * Close a large object file descriptor.
     *
     * @throws QueryException
     */
    public function loClose(int $fd): void
    {
        $this->query("SELECT lo_close($fd)");
    }

    /**
     * Delete a large object.
     *
     * @throws QueryException
     */
    public function loUnlink(int $oid): void
    {
        $this->query("SELECT lo_unlink($oid)");
    }

    // ========================================================================
    // Advisory Locks
    // ========================================================================

    /**
     * Acquire a session-level advisory lock.
     *
     * @throws QueryException
     */
    public function advisoryLock(int $key, bool $shared = false): void
    {
        $fn = $shared ? 'pg_advisory_lock_shared' : 'pg_advisory_lock';
        $this->query("SELECT $fn($key)");
    }

    /**
     * Try to acquire an advisory lock without blocking.
     *
     * @throws QueryException
     */
    public function advisoryLockTry(int $key, bool $shared = false): bool
    {
        $fn = $shared ? 'pg_try_advisory_lock_shared' : 'pg_try_advisory_lock';
        $result = $this->query("SELECT $fn($key) AS acquired");
        return (bool)$result->rows[0]['acquired'];
    }

    /**
     * Release a session-level advisory lock.
     *
     * @throws QueryException
     */
    public function advisoryUnlock(int $key, bool $shared = false): bool
    {
        $fn = $shared ? 'pg_advisory_unlock_shared' : 'pg_advisory_unlock';
        $result = $this->query("SELECT $fn($key) AS released");
        return (bool)$result->rows[0]['released'];
    }

    /**
     * Release all session-level advisory locks.
     *
     * @throws QueryException
     */
    public function advisoryUnlockAll(): void
    {
        $this->query("SELECT pg_advisory_unlock_all()");
    }

    // ========================================================================
    // Full-Text Search
    // ========================================================================

    /**
     * Perform a full-text search.
     *
     * @throws QueryException
     */
    public function ftsSearch(
        string $table,
        string $textColumn,
        string $queryStr,
        string $config = 'english',
        int $limit = 100,
        bool $includeRank = true,
        ?string $where = null
    ): QueryResult {
        $safeTable = $this->escapeIdentifier($table);
        $safeColumn = $this->escapeIdentifier($textColumn);
        $safeQuery = $this->escapeString($queryStr);

        if ($includeRank) {
            $sql = "SELECT *, ts_rank(to_tsvector('$config', $safeColumn), plainto_tsquery('$config', '$safeQuery')) AS rank ";
            $sql .= "FROM $safeTable WHERE to_tsvector('$config', $safeColumn) @@ plainto_tsquery('$config', '$safeQuery')";
        } else {
            $sql = "SELECT * FROM $safeTable WHERE to_tsvector('$config', $safeColumn) @@ plainto_tsquery('$config', '$safeQuery')";
        }
        if ($where !== null) $sql .= " AND $where";
        if ($includeRank) $sql .= " ORDER BY rank DESC";
        $sql .= " LIMIT $limit";

        return $this->query($sql);
    }

    // ========================================================================
    // Geospatial Support
    // ========================================================================

    /**
     * Find records within a distance from a point.
     *
     * @throws QueryException
     */
    public function geoDistance(
        string $table,
        string $geoColumn,
        float $lat,
        float $lon,
        float $radiusMeters,
        int $limit = 100,
        ?string $where = null
    ): QueryResult {
        $safeTable = $this->escapeIdentifier($table);
        $safeColumn = $this->escapeIdentifier($geoColumn);

        $sql = "SELECT *, ST_Distance($safeColumn, ST_SetSRID(ST_MakePoint($lon, $lat), 4326)::geography) AS distance ";
        $sql .= "FROM $safeTable WHERE ST_DWithin($safeColumn, ST_SetSRID(ST_MakePoint($lon, $lat), 4326)::geography, $radiusMeters)";
        if ($where !== null) $sql .= " AND $where";
        $sql .= " ORDER BY distance ASC LIMIT $limit";

        return $this->query($sql);
    }

    /**
     * Find K nearest neighbors to a point.
     *
     * @throws QueryException
     */
    public function geoNearest(
        string $table,
        string $geoColumn,
        float $lat,
        float $lon,
        int $k = 10,
        ?string $where = null
    ): QueryResult {
        $safeTable = $this->escapeIdentifier($table);
        $safeColumn = $this->escapeIdentifier($geoColumn);

        $sql = "SELECT *, ST_Distance($safeColumn, ST_SetSRID(ST_MakePoint($lon, $lat), 4326)::geography) AS distance FROM $safeTable";
        if ($where !== null) $sql .= " WHERE $where";
        $sql .= " ORDER BY $safeColumn <-> ST_SetSRID(ST_MakePoint($lon, $lat), 4326)::geometry LIMIT $k";

        return $this->query($sql);
    }

    // ========================================================================
    // Time Travel Queries
    // ========================================================================

    /**
     * Query data as of a specific timestamp.
     *
     * @param string $timestamp ISO-8601 timestamp
     * @throws QueryException
     */
    public function queryAsOf(string $sql, string $timestamp): QueryResult
    {
        $safeTs = $this->escapeString($timestamp);
        return $this->query("$sql AS OF TIMESTAMP '$safeTs'");
    }

    /**
     * Query data changes between two timestamps.
     *
     * @throws QueryException
     */
    public function queryBetween(string $sql, string $startTime, string $endTime): QueryResult
    {
        $safeStart = $this->escapeString($startTime);
        $safeEnd = $this->escapeString($endTime);
        return $this->query("$sql FOR SYSTEM_TIME FROM '$safeStart' TO '$safeEnd'");
    }

    // ========================================================================
    // Batch Operations
    // ========================================================================

    /**
     * Insert multiple rows in batches.
     *
     * @param array<array<string, mixed>> $rows
     * @throws QueryException
     */
    public function batchInsert(string $table, array $rows, int $chunkSize = 1000): int
    {
        if (count($rows) === 0) return 0;

        $safeTable = $this->escapeIdentifier($table);
        $columns = array_keys($rows[0]);
        $colList = implode(', ', array_map([$this, 'escapeIdentifier'], $columns));

        $totalInserted = 0;
        foreach (array_chunk($rows, $chunkSize) as $chunk) {
            $values = [];
            foreach ($chunk as $row) {
                $vals = array_map([$this, 'formatValue'], array_values($row));
                $values[] = '(' . implode(', ', $vals) . ')';
            }
            $this->exec("INSERT INTO $safeTable ($colList) VALUES " . implode(', ', $values));
            $totalInserted += count($chunk);
        }
        return $totalInserted;
    }

    /**
     * Upsert multiple rows (INSERT ... ON CONFLICT).
     *
     * @param array<array<string, mixed>> $rows
     * @param array<string> $conflictColumns
     * @param array<string>|null $updateColumns
     * @throws QueryException
     */
    public function batchUpsert(
        string $table,
        array $rows,
        array $conflictColumns,
        ?array $updateColumns = null,
        int $chunkSize = 1000
    ): int {
        if (count($rows) === 0) return 0;

        $safeTable = $this->escapeIdentifier($table);
        $columns = array_keys($rows[0]);
        $colList = implode(', ', array_map([$this, 'escapeIdentifier'], $columns));
        $conflictList = implode(', ', array_map([$this, 'escapeIdentifier'], $conflictColumns));

        $conflictSet = array_flip($conflictColumns);
        $updateCols = $updateColumns ?? array_filter($columns, fn($c) => !isset($conflictSet[$c]));
        $updateClause = implode(', ', array_map(
            fn($c) => $this->escapeIdentifier($c) . ' = EXCLUDED.' . $this->escapeIdentifier($c),
            $updateCols
        ));

        $totalAffected = 0;
        foreach (array_chunk($rows, $chunkSize) as $chunk) {
            $values = [];
            foreach ($chunk as $row) {
                $vals = array_map([$this, 'formatValue'], array_values($row));
                $values[] = '(' . implode(', ', $vals) . ')';
            }

            $sql = "INSERT INTO $safeTable ($colList) VALUES " . implode(', ', $values) . " ON CONFLICT ($conflictList)";
            $sql .= $updateClause ? " DO UPDATE SET $updateClause" : " DO NOTHING";

            $this->exec($sql);
            $totalAffected += count($chunk);
        }
        return $totalAffected;
    }

    /**
     * Delete multiple rows by key values.
     *
     * @param array<mixed> $keys
     * @throws QueryException
     */
    public function batchDelete(string $table, array $keys, string $keyColumn, int $chunkSize = 1000): int
    {
        if (count($keys) === 0) return 0;

        $safeTable = $this->escapeIdentifier($table);
        $safeKeyColumn = $this->escapeIdentifier($keyColumn);

        $totalDeleted = 0;
        foreach (array_chunk($keys, $chunkSize) as $chunk) {
            $values = implode(', ', array_map([$this, 'formatValue'], $chunk));
            $this->exec("DELETE FROM $safeTable WHERE $safeKeyColumn IN ($values)");
            $totalDeleted += count($chunk);
        }
        return $totalDeleted;
    }

    // ========================================================================
    // Schema Introspection
    // ========================================================================

    /**
     * Get column information for a table.
     *
     * @throws QueryException
     */
    public function getColumns(string $table, ?string $database = null): QueryResult
    {
        $safeTable = $this->escapeIdentifier($table);
        $sql = $database !== null
            ? "DESCRIBE " . $this->escapeIdentifier($database) . ".$safeTable"
            : "DESCRIBE $safeTable";
        return $this->query($sql);
    }

    /**
     * Get indexes for a table.
     *
     * @throws QueryException
     */
    public function getIndexes(string $table, ?string $database = null): QueryResult
    {
        $safeTable = $this->escapeIdentifier($table);
        $sql = $database !== null
            ? "SHOW INDEXES ON " . $this->escapeIdentifier($database) . ".$safeTable"
            : "SHOW INDEXES ON $safeTable";
        return $this->query($sql);
    }

    /**
     * Get constraints for a table.
     *
     * @throws QueryException
     */
    public function getConstraints(string $table, ?string $database = null): QueryResult
    {
        $safeTable = $this->escapeIdentifier($table);
        $sql = $database !== null
            ? "SHOW CONSTRAINTS ON " . $this->escapeIdentifier($database) . ".$safeTable"
            : "SHOW CONSTRAINTS ON $safeTable";
        return $this->query($sql);
    }

    // ========================================================================
    // Array Operations
    // ========================================================================

    /**
     * Find rows where array column contains a value.
     *
     * @throws QueryException
     */
    public function arrayContains(
        string $table,
        string $arrayColumn,
        mixed $value,
        int $limit = 100,
        ?string $where = null
    ): QueryResult {
        $sql = "SELECT * FROM " . $this->escapeIdentifier($table) .
               " WHERE " . $this->formatValue($value) . " = ANY(" . $this->escapeIdentifier($arrayColumn) . ")";
        if ($where !== null) $sql .= " AND $where";
        $sql .= " LIMIT $limit";
        return $this->query($sql);
    }

    /**
     * Find rows where array columns have overlapping elements.
     *
     * @param array<mixed> $values
     * @throws QueryException
     */
    public function arrayOverlap(
        string $table,
        string $arrayColumn,
        array $values,
        int $limit = 100,
        ?string $where = null
    ): QueryResult {
        $arrayLiteral = "ARRAY[" . implode(", ", array_map([$this, 'formatValue'], $values)) . "]";
        $sql = "SELECT * FROM " . $this->escapeIdentifier($table) .
               " WHERE " . $this->escapeIdentifier($arrayColumn) . " && $arrayLiteral";
        if ($where !== null) $sql .= " AND $where";
        $sql .= " LIMIT $limit";
        return $this->query($sql);
    }

    // ========================================================================
    // Async Pub/Sub Polling
    // ========================================================================

    /**
     * Poll for pending notifications.
     *
     * @return array<array{channel: string, payload: string}>
     * @throws QueryException
     */
    public function pollNotifications(int $timeoutMs = 0): array
    {
        $result = $this->query("SELECT * FROM pg_notification_queue($timeoutMs)");
        $notifications = [];
        foreach ($result->rows as $row) {
            $notifications[] = [
                'channel' => $row['channel'] ?? '',
                'payload' => $row['payload'] ?? '',
            ];
        }
        return $notifications;
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    /**
     * Escape an identifier for SQL.
     */
    private function escapeIdentifier(string $identifier): string
    {
        if (str_contains($identifier, '.')) {
            return implode('.', array_map([$this, 'escapeIdentifier'], explode('.', $identifier)));
        }
        return '"' . str_replace('"', '""', $identifier) . '"';
    }

    /**
     * Escape a string value for SQL.
     */
    private function escapeString(string $value): string
    {
        return str_replace("'", "''", $value);
    }

    /**
     * Format a value for SQL.
     */
    private function formatValue(mixed $value): string
    {
        if ($value === null) {
            return 'NULL';
        }
        if (is_bool($value)) {
            return $value ? 'TRUE' : 'FALSE';
        }
        if (is_int($value) || is_float($value)) {
            return (string)$value;
        }
        if (is_string($value)) {
            return "'" . $this->escapeString($value) . "'";
        }
        // Arrays and objects as JSON
        return "'" . $this->escapeString(json_encode($value)) . "'";
    }

    /**
     * Execute a callable within a transaction.
     *
     * @param callable $fn Function to execute
     * @return mixed Return value of the function
     * @throws \Throwable
     */
    public function inTransaction(callable $fn): mixed
    {
        $this->begin();
        try {
            $result = $fn();
            $this->commit();
            return $result;
        } catch (\Throwable $e) {
            $this->rollback();
            throw $e;
        }
    }

    // Vector Search Support

    /**
     * Perform vector similarity search.
     *
     * @param array<float> $queryVector Query embedding vector
     * @param string $table Table to search
     * @param string $vectorColumn Column containing embeddings
     * @param string $idColumn Column containing row IDs
     * @param string $metric Distance metric: "cosine", "euclidean", "dot", "manhattan"
     * @param int $limit Maximum number of results
     * @param string|null $filterClause Optional SQL WHERE clause
     * @param array<string>|null $selectColumns Additional columns to return
     * @return QueryResult
     * @throws QueryException
     */
    public function vectorSearch(
        array $queryVector,
        string $table,
        string $vectorColumn = 'embedding',
        string $idColumn = 'id',
        string $metric = 'cosine',
        int $limit = 10,
        ?string $filterClause = null,
        ?array $selectColumns = null
    ): QueryResult {
        $vectorStr = $this->formatVector($queryVector);

        $cols = [$idColumn];
        if ($selectColumns !== null) {
            $cols = array_merge($cols, $selectColumns);
        }
        $selectList = implode(', ', $cols);

        if ($metric === 'cosine') {
            $sql = "SELECT $selectList, similarity($vectorColumn, $vectorStr) AS score FROM $table";
            $order = 'DESC';
        } else {
            $sql = "SELECT $selectList, vector_distance($vectorColumn, $vectorStr, '$metric') AS score FROM $table";
            $order = 'ASC';
        }

        if ($filterClause !== null) {
            $sql .= " WHERE $filterClause";
        }

        $sql .= " ORDER BY score $order LIMIT $limit";

        return $this->query($sql);
    }

    /**
     * Perform hybrid search combining vector similarity and text search.
     *
     * @param array<float> $queryVector Query embedding vector
     * @param string $textQuery Text query for BM25 search
     * @param string $table Table to search
     * @param string $vectorColumn Column containing embeddings
     * @param string $textColumn Column containing text
     * @param string $idColumn Column containing row IDs
     * @param float $vectorWeight Weight for vector similarity (0.0-1.0)
     * @param float $textWeight Weight for text relevance (0.0-1.0)
     * @param int $limit Maximum number of results
     * @param string|null $filterClause Optional SQL WHERE clause
     * @param array<string>|null $selectColumns Additional columns to return
     * @return QueryResult
     * @throws QueryException
     */
    public function hybridSearch(
        array $queryVector,
        string $textQuery,
        string $table,
        string $vectorColumn = 'embedding',
        string $textColumn = 'content',
        string $idColumn = 'id',
        float $vectorWeight = 0.5,
        float $textWeight = 0.5,
        int $limit = 10,
        ?string $filterClause = null,
        ?array $selectColumns = null
    ): QueryResult {
        $vectorStr = $this->formatVector($queryVector);
        $escapedText = str_replace("'", "''", $textQuery);

        $cols = [$idColumn];
        if ($selectColumns !== null) {
            $cols = array_merge($cols, $selectColumns);
        }
        $selectList = implode(', ', $cols);

        $sql = <<<SQL
            SELECT $selectList,
                   similarity($vectorColumn, $vectorStr) * $vectorWeight AS vector_score,
                   COALESCE(match_score($textColumn, '$escapedText'), 0) * $textWeight AS text_score,
                   similarity($vectorColumn, $vectorStr) * $vectorWeight +
                   COALESCE(match_score($textColumn, '$escapedText'), 0) * $textWeight AS combined_score
            FROM $table
            SQL;

        if ($filterClause !== null) {
            $sql .= " WHERE $filterClause";
        }

        $sql .= " ORDER BY combined_score DESC LIMIT $limit";

        return $this->query($sql);
    }

    /**
     * Format a vector as a SQL array literal.
     *
     * @param array<float> $vector
     * @return string
     */
    private function formatVector(array $vector): string
    {
        $values = implode(',', array_map('strval', $vector));
        return "ARRAY[$values]";
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
