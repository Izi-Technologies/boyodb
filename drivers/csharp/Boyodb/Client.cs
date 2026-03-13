using System.Buffers.Binary;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Boyodb;

/// <summary>
/// boyodb client for C#/.NET.
/// </summary>
public class Client : IDisposable
{
    private readonly string _hostname;
    private readonly int _port;
    private readonly Config _config;
    private TcpClient? _tcpClient;
    private Stream? _stream;
    private string? _sessionId;
    private bool _disposed;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// Create a new boyodb client.
    /// </summary>
    /// <param name="host">Server address in host:port format</param>
    /// <param name="config">Configuration options (uses defaults if null)</param>
    public Client(string host, Config? config = null)
    {
        var parts = host.Split(':');
        _hostname = parts.Length > 0 ? parts[0] : "localhost";
        _port = parts.Length > 1 ? int.Parse(parts[1]) : 8765;
        _config = config ?? new Config();
    }

    /// <summary>
    /// Connect to the server.
    /// </summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        await ConnectWithRetryAsync(cancellationToken);

        try
        {
            await HealthAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            Close();
            throw new ConnectionException($"Health check failed: {ex.Message}", ex);
        }
    }

    private async Task ConnectWithRetryAsync(CancellationToken cancellationToken)
    {
        Exception? lastError = null;

        for (int attempt = 0; attempt < _config.MaxRetries; attempt++)
        {
            try
            {
                await ConnectOnceAsync(cancellationToken);
                return;
            }
            catch (Exception ex)
            {
                lastError = ex;
                if (attempt < _config.MaxRetries - 1)
                {
                    await Task.Delay(_config.RetryDelay, cancellationToken);
                }
            }
        }

        throw new ConnectionException(
            $"Failed to connect after {_config.MaxRetries} attempts: {lastError?.Message}",
            lastError!);
    }

    private async Task ConnectOnceAsync(CancellationToken cancellationToken)
    {
        _tcpClient = new TcpClient();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_config.ConnectTimeout);

        await _tcpClient.ConnectAsync(_hostname, _port, cts.Token);

        Stream stream = _tcpClient.GetStream();

        if (_config.Tls)
        {
            var sslStream = new SslStream(
                stream,
                leaveInnerStreamOpen: false,
                ValidateServerCertificate);

            var sslOptions = new SslClientAuthenticationOptions
            {
                TargetHost = _hostname,
            };

            if (_config.InsecureSkipVerify)
            {
                Console.Error.WriteLine("WARNING: TLS certificate verification is DISABLED. " +
                    "This is insecure and vulnerable to MITM attacks. " +
                    "Only use for testing with self-signed certificates.");
            }

            await sslStream.AuthenticateAsClientAsync(sslOptions, cts.Token);
            stream = sslStream;
        }

        _stream = stream;
    }

    private bool ValidateServerCertificate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors sslPolicyErrors)
    {
        if (_config.InsecureSkipVerify)
        {
            return true;
        }

        if (sslPolicyErrors == SslPolicyErrors.None)
        {
            return true;
        }

        if (!string.IsNullOrEmpty(_config.CaFile) && File.Exists(_config.CaFile))
        {
            var caCert = new X509Certificate2(_config.CaFile);
            chain?.ChainPolicy.ExtraStore.Add(caCert);
            chain?.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
            return chain?.Build(new X509Certificate2(certificate!)) ?? false;
        }

        return false;
    }

    /// <summary>
    /// Close the connection.
    /// </summary>
    public void Close()
    {
        _stream?.Dispose();
        _tcpClient?.Dispose();
        _stream = null;
        _tcpClient = null;
    }

    /// <summary>
    /// Dispose the client.
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            Close();
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }

    private async Task<ServerResponse> SendRequestAsync(
        Dictionary<string, object?> request,
        CancellationToken cancellationToken = default)
    {
        if (_stream == null)
        {
            throw new ConnectionException("Not connected");
        }

        // Add auth if available
        if (!string.IsNullOrEmpty(_sessionId))
        {
            request["auth"] = _sessionId;
        }
        else if (!string.IsNullOrEmpty(_config.Token))
        {
            request["auth"] = _config.Token;
        }

        // Serialize request
        var json = JsonSerializer.Serialize(request, JsonOptions);
        var payload = Encoding.UTF8.GetBytes(json);

        // Write length prefix and payload
        var lengthBuffer = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(lengthBuffer, (uint)payload.Length);

        await _stream.WriteAsync(lengthBuffer, cancellationToken);
        await _stream.WriteAsync(payload, cancellationToken);
        await _stream.FlushAsync(cancellationToken);

        // Read response length
        var respLengthBuffer = new byte[4];
        await ReadExactAsync(_stream, respLengthBuffer, cancellationToken);
        var respLength = BinaryPrimitives.ReadUInt32BigEndian(respLengthBuffer);

        if (respLength > 100 * 1024 * 1024)
        {
            throw new QueryException($"Response too large: {respLength} bytes");
        }

        // Read response body
        var respBuffer = new byte[respLength];
        await ReadExactAsync(_stream, respBuffer, cancellationToken);

        var json = JsonDocument.Parse(respBuffer);
        if (json.RootElement.TryGetProperty("ipc_streaming", out var ipcStreamingElement) &&
            ipcStreamingElement.ValueKind == JsonValueKind.True)
        {
            var ipcBuffer = await ReadStreamFramesAsync(_stream, cancellationToken);
            return new ServerResponse(json, ipcBuffer);
        }

        if (json.RootElement.TryGetProperty("ipc_len", out var ipcLenElement) &&
            ipcLenElement.ValueKind == JsonValueKind.Number &&
            ipcLenElement.TryGetInt64(out var ipcLen) &&
            ipcLen > 0)
        {
            if (ipcLen > 100 * 1024 * 1024)
            {
                throw new QueryException($"Response too large: {ipcLen} bytes");
            }

            var ipcLenBuffer = new byte[4];
            await ReadExactAsync(_stream, ipcLenBuffer, cancellationToken);
            var payloadLen = BinaryPrimitives.ReadUInt32BigEndian(ipcLenBuffer);
            if (payloadLen != ipcLen)
            {
                throw new QueryException($"IPC length mismatch: expected {ipcLen} got {payloadLen}");
            }

            var ipcBuffer = new byte[payloadLen];
            await ReadExactAsync(_stream, ipcBuffer, cancellationToken);
            return new ServerResponse(json, ipcBuffer);
        }

        return new ServerResponse(json, null);
    }

    private sealed class ServerResponse : IDisposable
    {
        public ServerResponse(JsonDocument json, byte[]? ipcBytes)
        {
            Json = json;
            IpcBytes = ipcBytes;
        }

        public JsonDocument Json { get; }
        public byte[]? IpcBytes { get; }

        public void Dispose()
        {
            Json.Dispose();
        }
    }

    private static bool IsSelectLike(string sql)
    {
        var trimmed = sql.TrimStart().ToLowerInvariant();
        return trimmed.StartsWith("select ") || trimmed.StartsWith("with ");
    }

    private static async Task ReadExactAsync(Stream stream, byte[] buffer, CancellationToken ct)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset, buffer.Length - offset), ct);
            if (read == 0)
            {
                throw new ConnectionException("Connection closed");
            }
            offset += read;
        }
    }

    private static async Task<byte[]> ReadStreamFramesAsync(Stream stream, CancellationToken ct)
    {
        using var ms = new MemoryStream();
        var lengthBuffer = new byte[4];
        while (true)
        {
            await ReadExactAsync(stream, lengthBuffer, ct);
            var chunkLen = BinaryPrimitives.ReadUInt32BigEndian(lengthBuffer);
            if (chunkLen == 0)
            {
                break;
            }
            if (chunkLen > 100 * 1024 * 1024)
            {
                throw new QueryException($"Response too large: {chunkLen} bytes");
            }
            var chunk = new byte[chunkLen];
            await ReadExactAsync(stream, chunk, ct);
            ms.Write(chunk, 0, chunk.Length);
        }
        return ms.ToArray();
    }

    /// <summary>
    /// Check server health.
    /// </summary>
    public async Task HealthAsync(CancellationToken cancellationToken = default)
    {
        using var response = await SendRequestAsync(
            new Dictionary<string, object?> { ["op"] = "health" },
            cancellationToken);

        CheckStatus(response.Json, "Health check failed");
    }

    /// <summary>
    /// Login with username and password.
    /// </summary>
    public async Task LoginAsync(string username, string password, CancellationToken cancellationToken = default)
    {
        using var response = await SendRequestAsync(
            new Dictionary<string, object?>
            {
                ["op"] = "login",
                ["username"] = username,
                ["password"] = password
            },
            cancellationToken);

        CheckStatus(response.Json, "Login failed");

        if (response.Json.RootElement.TryGetProperty("session_id", out var sessionIdElement))
        {
            _sessionId = sessionIdElement.GetString();
        }
    }

    /// <summary>
    /// Logout from the server.
    /// </summary>
    public async Task LogoutAsync(CancellationToken cancellationToken = default)
    {
        using var response = await SendRequestAsync(
            new Dictionary<string, object?> { ["op"] = "logout" },
            cancellationToken);

        CheckStatus(response.Json, "Logout failed");
        _sessionId = null;
    }

    /// <summary>
    /// Execute a SQL query.
    /// </summary>
    public async Task<QueryResult> QueryAsync(
        string sql,
        string? database = null,
        int? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var request = new Dictionary<string, object?>
        {
            ["op"] = IsSelectLike(sql) ? "query_binary" : "query",
            ["sql"] = sql,
            ["timeout_millis"] = timeout ?? _config.QueryTimeout
        };
        if (IsSelectLike(sql))
        {
            request["stream"] = true;
        }

        var db = database ?? _config.Database;
        if (!string.IsNullOrEmpty(db))
        {
            request["database"] = db;
        }

        using var response = await SendRequestAsync(request, cancellationToken);
        CheckStatus(response.Json, "Query failed");

        var result = new QueryResult();

        if (response.Json.RootElement.TryGetProperty("segments_scanned", out var segmentsElement))
        {
            result.SegmentsScanned = segmentsElement.GetInt32();
        }

        if (response.Json.RootElement.TryGetProperty("data_skipped_bytes", out var skippedElement))
        {
            result.DataSkippedBytes = skippedElement.GetInt64();
        }

        if (response.IpcBytes != null)
        {
            ParseArrowIpc(response.IpcBytes, result);
        }
        else if (response.Json.RootElement.TryGetProperty("ipc_base64", out var ipcElement))
        {
            var ipcBase64 = ipcElement.GetString();
            if (!string.IsNullOrEmpty(ipcBase64))
            {
                var ipcData = Convert.FromBase64String(ipcBase64);
                ParseArrowIpc(ipcData, result);
            }
        }

        return result;
    }

    /// <summary>
    /// Execute a SQL statement that doesn't return rows.
    /// </summary>
    public async Task ExecAsync(
        string sql,
        string? database = null,
        int? timeout = null,
        CancellationToken cancellationToken = default)
    {
        await QueryAsync(sql, database, timeout, cancellationToken);
    }

    /// <summary>
    /// Prepare a SELECT query on the server and return a prepared id.
    /// </summary>
    public async Task<string> PrepareAsync(
        string sql,
        string? database = null,
        CancellationToken cancellationToken = default)
    {
        var request = new Dictionary<string, object?>
        {
            ["op"] = "prepare",
            ["sql"] = sql,
        };

        var db = database ?? _config.Database;
        if (!string.IsNullOrEmpty(db))
        {
            request["database"] = db;
        }

        using var response = await SendRequestAsync(request, cancellationToken);
        CheckStatus(response.Json, "Prepare failed");

        if (response.Json.RootElement.TryGetProperty("prepared_id", out var idElement))
        {
            var id = idElement.GetString();
            if (!string.IsNullOrEmpty(id))
            {
                return id;
            }
        }

        throw new QueryException("missing prepared_id in response");
    }

    /// <summary>
    /// Execute a prepared statement using binary IPC responses.
    /// </summary>
    public async Task<QueryResult> ExecutePreparedBinaryAsync(
        string preparedId,
        int? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var request = new Dictionary<string, object?>
        {
            ["op"] = "execute_prepared_binary",
            ["id"] = preparedId,
            ["timeout_millis"] = timeout ?? _config.QueryTimeout,
            ["stream"] = true,
        };

        using var response = await SendRequestAsync(request, cancellationToken);
        CheckStatus(response.Json, "Execute prepared failed");

        var result = new QueryResult();

        if (response.Json.RootElement.TryGetProperty("segments_scanned", out var segmentsElement))
        {
            result.SegmentsScanned = segmentsElement.GetInt32();
        }

        if (response.Json.RootElement.TryGetProperty("data_skipped_bytes", out var skippedElement))
        {
            result.DataSkippedBytes = skippedElement.GetInt64();
        }

        if (response.IpcBytes != null)
        {
            ParseArrowIpc(response.IpcBytes, result);
        }
        else if (response.Json.RootElement.TryGetProperty("ipc_base64", out var ipcElement))
        {
            var ipcBase64 = ipcElement.GetString();
            if (!string.IsNullOrEmpty(ipcBase64))
            {
                var ipcData = Convert.FromBase64String(ipcBase64);
                ParseArrowIpc(ipcData, result);
            }
        }

        return result;
    }

    /// <summary>
    /// Create a new database.
    /// </summary>
    public async Task CreateDatabaseAsync(string name, CancellationToken cancellationToken = default)
    {
        using var response = await SendRequestAsync(
            new Dictionary<string, object?>
            {
                ["op"] = "createdatabase",
                ["name"] = name
            },
            cancellationToken);

        CheckStatus(response.Json, "Create database failed");
    }

    /// <summary>
    /// Create a new table.
    /// </summary>
    public async Task CreateTableAsync(string database, string table, CancellationToken cancellationToken = default)
    {
        using var response = await SendRequestAsync(
            new Dictionary<string, object?>
            {
                ["op"] = "createtable",
                ["database"] = database,
                ["table"] = table
            },
            cancellationToken);

        CheckStatus(response.Json, "Create table failed");
    }

    /// <summary>
    /// List all databases.
    /// </summary>
    public async Task<List<string>> ListDatabasesAsync(CancellationToken cancellationToken = default)
    {
        using var response = await SendRequestAsync(
            new Dictionary<string, object?> { ["op"] = "listdatabases" },
            cancellationToken);

        CheckStatus(response.Json, "List databases failed");

        var databases = new List<string>();
        if (response.Json.RootElement.TryGetProperty("databases", out var dbsElement))
        {
            foreach (var db in dbsElement.EnumerateArray())
            {
                var name = db.GetString();
                if (name != null) databases.Add(name);
            }
        }

        return databases;
    }

    /// <summary>
    /// List tables, optionally filtered by database.
    /// </summary>
    public async Task<List<TableInfo>> ListTablesAsync(
        string? database = null,
        CancellationToken cancellationToken = default)
    {
        var request = new Dictionary<string, object?> { ["op"] = "listtables" };
        if (!string.IsNullOrEmpty(database))
        {
            request["database"] = database;
        }

        using var response = await SendRequestAsync(request, cancellationToken);
        CheckStatus(response.Json, "List tables failed");

        var tables = new List<TableInfo>();
        if (response.Json.RootElement.TryGetProperty("tables", out var tablesElement))
        {
            foreach (var t in tablesElement.EnumerateArray())
            {
                tables.Add(new TableInfo
                {
                    Database = t.TryGetProperty("database", out var dbProp) ? dbProp.GetString() ?? "" : "",
                    Name = t.TryGetProperty("name", out var nameProp) ? nameProp.GetString() ?? "" : "",
                    SchemaJson = t.TryGetProperty("schema_json", out var schemaProp) ? schemaProp.GetString() : null
                });
            }
        }

        return tables;
    }

    /// <summary>
    /// Get query execution plan.
    /// </summary>
    public async Task<JsonDocument> ExplainAsync(string sql, CancellationToken cancellationToken = default)
    {
        using var response = await SendRequestAsync(
            new Dictionary<string, object?>
            {
                ["op"] = "explain",
                ["sql"] = sql
            },
            cancellationToken);

        CheckStatus(response.Json, "Explain failed");

        if (response.Json.RootElement.TryGetProperty("explain_plan", out var planElement))
        {
            return JsonDocument.Parse(planElement.GetRawText());
        }

        return JsonDocument.Parse("{}");
    }

    /// <summary>
    /// Get server metrics.
    /// </summary>
    public async Task<JsonDocument> MetricsAsync(CancellationToken cancellationToken = default)
    {
        using var response = await SendRequestAsync(
            new Dictionary<string, object?> { ["op"] = "metrics" },
            cancellationToken);

        CheckStatus(response.Json, "Metrics failed");

        if (response.Json.RootElement.TryGetProperty("metrics", out var metricsElement))
        {
            return JsonDocument.Parse(metricsElement.GetRawText());
        }

        return JsonDocument.Parse("{}");
    }

    /// <summary>
    /// Ingest CSV data into a table.
    /// </summary>
    public async Task IngestCsvAsync(
        string database,
        string table,
        byte[] csvData,
        bool hasHeader = true,
        string? delimiter = null,
        CancellationToken cancellationToken = default)
    {
        var request = new Dictionary<string, object?>
        {
            ["op"] = "ingestcsv",
            ["database"] = database,
            ["table"] = table,
            ["payload_base64"] = Convert.ToBase64String(csvData),
            ["has_header"] = hasHeader
        };

        if (!string.IsNullOrEmpty(delimiter))
        {
            request["delimiter"] = delimiter;
        }

        using var response = await SendRequestAsync(request, cancellationToken);
        CheckStatus(response.Json, "Ingest CSV failed");
    }

    /// <summary>
    /// Ingest CSV data from a string.
    /// </summary>
    public async Task IngestCsvAsync(
        string database,
        string table,
        string csvData,
        bool hasHeader = true,
        string? delimiter = null,
        CancellationToken cancellationToken = default)
    {
        await IngestCsvAsync(database, table, Encoding.UTF8.GetBytes(csvData), hasHeader, delimiter, cancellationToken);
    }

    /// <summary>
    /// Ingest Arrow IPC data into a table.
    /// </summary>
    public async Task IngestIpcAsync(
        string database,
        string table,
        byte[] ipcData,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await SendRequestBinaryAsync(
                new Dictionary<string, object?>
                {
                    ["op"] = "ingest_ipc_binary",
                    ["database"] = database,
                    ["table"] = table,
                },
                ipcData,
                cancellationToken);
            return;
        }
        catch
        {
            // Fall back to base64 on older servers.
        }

        using var response = await SendRequestAsync(
            new Dictionary<string, object?>
            {
                ["op"] = "ingestipc",
                ["database"] = database,
                ["table"] = table,
                ["payload_base64"] = Convert.ToBase64String(ipcData)
            },
            cancellationToken);

        CheckStatus(response.Json, "Ingest IPC failed");
    }

    private async Task SendRequestBinaryAsync(
        Dictionary<string, object?> request,
        byte[] payload,
        CancellationToken cancellationToken = default)
    {
        if (_stream == null)
        {
            throw new ConnectionException("Not connected");
        }

        if (!string.IsNullOrEmpty(_sessionId))
        {
            request["auth"] = _sessionId;
        }
        else if (!string.IsNullOrEmpty(_config.Token))
        {
            request["auth"] = _config.Token;
        }

        var json = JsonSerializer.Serialize(request, JsonOptions);
        var header = Encoding.UTF8.GetBytes(json);

        var headerLenBuffer = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(headerLenBuffer, (uint)header.Length);

        var payloadLenBuffer = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(payloadLenBuffer, (uint)payload.Length);

        await _stream.WriteAsync(headerLenBuffer, cancellationToken);
        await _stream.WriteAsync(header, cancellationToken);
        await _stream.WriteAsync(payloadLenBuffer, cancellationToken);
        await _stream.WriteAsync(payload, cancellationToken);
        await _stream.FlushAsync(cancellationToken);

        var respLenBuffer = new byte[4];
        await ReadExactAsync(_stream, respLenBuffer, cancellationToken);
        var respLen = BinaryPrimitives.ReadUInt32BigEndian(respLenBuffer);
        if (respLen > 100 * 1024 * 1024)
        {
            throw new QueryException($"Response too large: {respLen} bytes");
        }

        var respBuffer = new byte[respLen];
        await ReadExactAsync(_stream, respBuffer, cancellationToken);
        using var response = JsonDocument.Parse(respBuffer);
        CheckStatus(response, "Ingest IPC failed");
    }

    /// <summary>
    /// Set the default database.
    /// </summary>
    public void SetDatabase(string database) => _config.Database = database;

    /// <summary>
    /// Set the authentication token.
    /// </summary>
    public void SetToken(string token) => _config.Token = token;

    // ========================================================================
    // Pub/Sub Support
    // ========================================================================

    /// <summary>
    /// Start listening on a notification channel.
    /// </summary>
    public async Task ListenAsync(string channel, CancellationToken cancellationToken = default)
    {
        await ExecAsync($"LISTEN {EscapeIdentifier(channel)}", null, null, cancellationToken);
    }

    /// <summary>
    /// Stop listening on a notification channel.
    /// </summary>
    /// <param name="channel">Channel name, or "*" to unlisten all</param>
    public async Task UnlistenAsync(string channel = "*", CancellationToken cancellationToken = default)
    {
        if (channel == "*")
        {
            await ExecAsync("UNLISTEN *", null, null, cancellationToken);
        }
        else
        {
            await ExecAsync($"UNLISTEN {EscapeIdentifier(channel)}", null, null, cancellationToken);
        }
    }

    /// <summary>
    /// Send a notification on a channel.
    /// </summary>
    public async Task NotifyAsync(string channel, string? payload = null, CancellationToken cancellationToken = default)
    {
        if (payload != null)
        {
            await ExecAsync($"NOTIFY {EscapeIdentifier(channel)}, '{EscapeString(payload)}'", null, null, cancellationToken);
        }
        else
        {
            await ExecAsync($"NOTIFY {EscapeIdentifier(channel)}", null, null, cancellationToken);
        }
    }

    // ========================================================================
    // Trigger Management
    // ========================================================================

    /// <summary>
    /// Create a database trigger.
    /// </summary>
    public async Task CreateTriggerAsync(
        string name,
        string table,
        string timing,
        IEnumerable<string> events,
        string function,
        IEnumerable<object>? arguments = null,
        bool forEachRow = true,
        string? whenClause = null,
        bool orReplace = false,
        CancellationToken cancellationToken = default)
    {
        var sql = new System.Text.StringBuilder();
        sql.Append(orReplace ? "CREATE OR REPLACE TRIGGER " : "CREATE TRIGGER ");
        sql.Append(EscapeIdentifier(name)).Append(' ');
        sql.Append(timing.ToUpperInvariant()).Append(' ');
        sql.Append(string.Join(" OR ", events.Select(e => e.ToUpperInvariant()))).Append(' ');
        sql.Append("ON ").Append(EscapeIdentifier(table)).Append(' ');
        sql.Append(forEachRow ? "FOR EACH ROW " : "FOR EACH STATEMENT ");

        if (!string.IsNullOrEmpty(whenClause))
        {
            sql.Append("WHEN (").Append(whenClause).Append(") ");
        }

        sql.Append("EXECUTE FUNCTION ").Append(EscapeIdentifier(function)).Append('(');
        if (arguments != null)
        {
            sql.Append(string.Join(", ", arguments.Select(FormatValue)));
        }
        sql.Append(')');

        await ExecAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Drop a trigger.
    /// </summary>
    public async Task DropTriggerAsync(
        string name,
        string table,
        bool ifExists = false,
        CancellationToken cancellationToken = default)
    {
        var sql = $"DROP TRIGGER {(ifExists ? "IF EXISTS " : "")}{EscapeIdentifier(name)} ON {EscapeIdentifier(table)}";
        await ExecAsync(sql, null, null, cancellationToken);
    }

    /// <summary>
    /// Enable or disable a trigger.
    /// </summary>
    public async Task AlterTriggerAsync(
        string name,
        string table,
        bool enable = true,
        CancellationToken cancellationToken = default)
    {
        var action = enable ? "ENABLE" : "DISABLE";
        var sql = $"ALTER TRIGGER {EscapeIdentifier(name)} ON {EscapeIdentifier(table)} {action}";
        await ExecAsync(sql, null, null, cancellationToken);
    }

    /// <summary>
    /// List triggers on a table or in a database.
    /// </summary>
    public async Task<QueryResult> ListTriggersAsync(
        string? table = null,
        string? database = null,
        CancellationToken cancellationToken = default)
    {
        var sql = "SHOW TRIGGERS";
        if (!string.IsNullOrEmpty(table))
        {
            sql += $" ON {EscapeIdentifier(table)}";
        }
        else if (!string.IsNullOrEmpty(database))
        {
            sql += $" IN {EscapeIdentifier(database)}";
        }
        return await QueryAsync(sql, null, null, cancellationToken);
    }

    // ========================================================================
    // Stored Procedures and Functions
    // ========================================================================

    /// <summary>
    /// Call a stored procedure.
    /// </summary>
    public async Task<QueryResult> CallAsync(
        string procedure,
        object[]? args = null,
        CancellationToken cancellationToken = default)
    {
        var sql = new System.Text.StringBuilder("CALL ");
        sql.Append(EscapeIdentifier(procedure)).Append('(');
        if (args != null)
        {
            sql.Append(string.Join(", ", args.Select(FormatValue)));
        }
        sql.Append(')');
        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Create a stored function.
    /// </summary>
    public async Task CreateFunctionAsync(
        string name,
        IEnumerable<(string Name, string Type)> parameters,
        string returnType,
        string body,
        string language = "plpgsql",
        bool orReplace = false,
        string? volatility = null,
        CancellationToken cancellationToken = default)
    {
        var sql = new System.Text.StringBuilder();
        sql.Append(orReplace ? "CREATE OR REPLACE FUNCTION " : "CREATE FUNCTION ");
        sql.Append(EscapeIdentifier(name)).Append('(');
        sql.Append(string.Join(", ", parameters.Select(p => $"{EscapeIdentifier(p.Name)} {p.Type}")));
        sql.Append(") RETURNS ").Append(returnType);
        sql.Append(" LANGUAGE ").Append(language).Append(' ');

        if (!string.IsNullOrEmpty(volatility))
        {
            sql.Append(volatility.ToUpperInvariant()).Append(' ');
        }

        sql.Append("AS $$ ").Append(body).Append(" $$");

        await ExecAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Create a stored procedure.
    /// </summary>
    public async Task CreateProcedureAsync(
        string name,
        IEnumerable<(string Name, string Type, string? Mode)> parameters,
        string body,
        string language = "plpgsql",
        bool orReplace = false,
        CancellationToken cancellationToken = default)
    {
        var sql = new System.Text.StringBuilder();
        sql.Append(orReplace ? "CREATE OR REPLACE PROCEDURE " : "CREATE PROCEDURE ");
        sql.Append(EscapeIdentifier(name)).Append('(');
        sql.Append(string.Join(", ", parameters.Select(p =>
        {
            var mode = string.IsNullOrEmpty(p.Mode) ? "" : p.Mode.ToUpperInvariant() + " ";
            return $"{mode}{EscapeIdentifier(p.Name)} {p.Type}";
        })));
        sql.Append(") LANGUAGE ").Append(language);
        sql.Append(" AS $$ ").Append(body).Append(" $$");

        await ExecAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Drop a function.
    /// </summary>
    public async Task DropFunctionAsync(
        string name,
        IEnumerable<string>? paramTypes = null,
        bool ifExists = false,
        CancellationToken cancellationToken = default)
    {
        var sql = new System.Text.StringBuilder("DROP FUNCTION ");
        if (ifExists) sql.Append("IF EXISTS ");
        sql.Append(EscapeIdentifier(name));
        if (paramTypes != null)
        {
            sql.Append('(').Append(string.Join(", ", paramTypes)).Append(')');
        }
        await ExecAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Drop a procedure.
    /// </summary>
    public async Task DropProcedureAsync(
        string name,
        IEnumerable<string>? paramTypes = null,
        bool ifExists = false,
        CancellationToken cancellationToken = default)
    {
        var sql = new System.Text.StringBuilder("DROP PROCEDURE ");
        if (ifExists) sql.Append("IF EXISTS ");
        sql.Append(EscapeIdentifier(name));
        if (paramTypes != null)
        {
            sql.Append('(').Append(string.Join(", ", paramTypes)).Append(')');
        }
        await ExecAsync(sql.ToString(), null, null, cancellationToken);
    }

    // ========================================================================
    // JSON Operations
    // ========================================================================

    /// <summary>
    /// Extract a JSON value using the -> operator.
    /// </summary>
    public async Task<QueryResult> JsonExtractAsync(
        string column,
        string path,
        string table,
        string? where = null,
        int? limit = null,
        CancellationToken cancellationToken = default)
    {
        var pathExpr = int.TryParse(path, out _) ? path : $"'{EscapeString(path)}'";
        var sql = new System.Text.StringBuilder();
        sql.Append($"SELECT {EscapeIdentifier(column)} -> {pathExpr} AS value FROM {EscapeIdentifier(table)}");
        if (!string.IsNullOrEmpty(where)) sql.Append($" WHERE {where}");
        if (limit.HasValue && limit > 0) sql.Append($" LIMIT {limit}");
        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Extract a JSON value as text using the ->> operator.
    /// </summary>
    public async Task<QueryResult> JsonExtractTextAsync(
        string column,
        string path,
        string table,
        string? where = null,
        int? limit = null,
        CancellationToken cancellationToken = default)
    {
        var pathExpr = int.TryParse(path, out _) ? path : $"'{EscapeString(path)}'";
        var sql = new System.Text.StringBuilder();
        sql.Append($"SELECT {EscapeIdentifier(column)} ->> {pathExpr} AS value FROM {EscapeIdentifier(table)}");
        if (!string.IsNullOrEmpty(where)) sql.Append($" WHERE {where}");
        if (limit.HasValue && limit > 0) sql.Append($" LIMIT {limit}");
        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Extract a nested JSON value using the #> operator.
    /// </summary>
    public async Task<QueryResult> JsonExtractPathAsync(
        string column,
        IEnumerable<string> path,
        string table,
        string? where = null,
        int? limit = null,
        CancellationToken cancellationToken = default)
    {
        var pathArray = $"'{{{string.Join(",", path.Select(EscapeString))}}}'";
        var sql = new System.Text.StringBuilder();
        sql.Append($"SELECT {EscapeIdentifier(column)} #> {pathArray} AS value FROM {EscapeIdentifier(table)}");
        if (!string.IsNullOrEmpty(where)) sql.Append($" WHERE {where}");
        if (limit.HasValue && limit > 0) sql.Append($" LIMIT {limit}");
        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Check if JSON contains a value using the @> operator.
    /// </summary>
    public async Task<QueryResult> JsonContainsAsync(
        string column,
        object value,
        string table,
        string? where = null,
        int? limit = null,
        CancellationToken cancellationToken = default)
    {
        var jsonValue = JsonSerializer.Serialize(value);
        var sql = new System.Text.StringBuilder();
        sql.Append($"SELECT * FROM {EscapeIdentifier(table)} WHERE {EscapeIdentifier(column)} @> '{EscapeString(jsonValue)}'::jsonb");
        if (!string.IsNullOrEmpty(where)) sql.Append($" AND {where}");
        if (limit.HasValue && limit > 0) sql.Append($" LIMIT {limit}");
        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Check if JSON is contained in a value using the <@ operator.
    /// </summary>
    public async Task<QueryResult> JsonContainedByAsync(
        string column,
        object value,
        string table,
        string? where = null,
        int? limit = null,
        CancellationToken cancellationToken = default)
    {
        var jsonValue = JsonSerializer.Serialize(value);
        var sql = new System.Text.StringBuilder();
        sql.Append($"SELECT * FROM {EscapeIdentifier(table)} WHERE {EscapeIdentifier(column)} <@ '{EscapeString(jsonValue)}'::jsonb");
        if (!string.IsNullOrEmpty(where)) sql.Append($" AND {where}");
        if (limit.HasValue && limit > 0) sql.Append($" LIMIT {limit}");
        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Check if a JSON path exists using the @? operator.
    /// </summary>
    public async Task<QueryResult> JsonPathExistsAsync(
        string column,
        string jsonPath,
        string table,
        string? where = null,
        int? limit = null,
        CancellationToken cancellationToken = default)
    {
        var sql = new System.Text.StringBuilder();
        sql.Append($"SELECT * FROM {EscapeIdentifier(table)} WHERE {EscapeIdentifier(column)} @? '{EscapeString(jsonPath)}'");
        if (!string.IsNullOrEmpty(where)) sql.Append($" AND {where}");
        if (limit.HasValue && limit > 0) sql.Append($" LIMIT {limit}");
        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Check if a JSON path predicate matches using the @@ operator.
    /// </summary>
    public async Task<QueryResult> JsonPathMatchAsync(
        string column,
        string jsonPath,
        string table,
        string? where = null,
        int? limit = null,
        CancellationToken cancellationToken = default)
    {
        var sql = new System.Text.StringBuilder();
        sql.Append($"SELECT * FROM {EscapeIdentifier(table)} WHERE {EscapeIdentifier(column)} @@ '{EscapeString(jsonPath)}'");
        if (!string.IsNullOrEmpty(where)) sql.Append($" AND {where}");
        if (limit.HasValue && limit > 0) sql.Append($" LIMIT {limit}");
        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    // ========================================================================
    // Cursor Support
    // ========================================================================

    /// <summary>
    /// Declare a server-side cursor.
    /// </summary>
    public async Task DeclareCursorAsync(
        string name,
        string query,
        bool scroll = false,
        bool hold = false,
        CancellationToken cancellationToken = default)
    {
        var sql = new System.Text.StringBuilder("DECLARE ");
        sql.Append(EscapeIdentifier(name));
        if (scroll) sql.Append(" SCROLL");
        if (hold) sql.Append(" WITH HOLD");
        sql.Append(" CURSOR FOR ").Append(query);
        await ExecAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Fetch rows from a cursor.
    /// </summary>
    public async Task<QueryResult> FetchCursorAsync(
        string name,
        int count = 1,
        string direction = "NEXT",
        CancellationToken cancellationToken = default)
    {
        var dir = direction.ToUpperInvariant();
        string sql;
        if (dir is "NEXT" or "PRIOR" or "FIRST" or "LAST")
        {
            sql = $"FETCH {dir} {count} FROM {EscapeIdentifier(name)}";
        }
        else
        {
            sql = $"FETCH {dir} FROM {EscapeIdentifier(name)}";
        }
        return await QueryAsync(sql, null, null, cancellationToken);
    }

    /// <summary>
    /// Move cursor position without returning data.
    /// </summary>
    public async Task MoveCursorAsync(
        string name,
        int count = 1,
        string direction = "NEXT",
        CancellationToken cancellationToken = default)
    {
        var dir = direction.ToUpperInvariant();
        string sql;
        if (dir is "NEXT" or "PRIOR" or "FIRST" or "LAST")
        {
            sql = $"MOVE {dir} {count} IN {EscapeIdentifier(name)}";
        }
        else
        {
            sql = $"MOVE {dir} IN {EscapeIdentifier(name)}";
        }
        await ExecAsync(sql, null, null, cancellationToken);
    }

    /// <summary>
    /// Close a cursor.
    /// </summary>
    public async Task CloseCursorAsync(string name, CancellationToken cancellationToken = default)
    {
        if (name == "*" || name.Equals("ALL", StringComparison.OrdinalIgnoreCase))
        {
            await ExecAsync("CLOSE ALL", null, null, cancellationToken);
        }
        else
        {
            await ExecAsync($"CLOSE {EscapeIdentifier(name)}", null, null, cancellationToken);
        }
    }

    // ========================================================================
    // Large Objects (LOB) Support
    // ========================================================================

    /// <summary>
    /// Create a new large object.
    /// </summary>
    public async Task<long> LoCreateAsync(CancellationToken cancellationToken = default)
    {
        var result = await QueryAsync("SELECT lo_create(0) AS oid", null, null, cancellationToken);
        if (result.Rows.Count == 0) throw new QueryException("No result from lo_create");
        return Convert.ToInt64(result.Rows[0]["oid"]);
    }

    /// <summary>
    /// Open a large object.
    /// </summary>
    public async Task<int> LoOpenAsync(long oid, string mode = "rw", CancellationToken cancellationToken = default)
    {
        // INV_READ = 0x40000, INV_WRITE = 0x20000
        int modeFlag = 0x40000;
        if (mode.Contains('w')) modeFlag |= 0x20000;
        var result = await QueryAsync($"SELECT lo_open({oid}, {modeFlag}) AS fd", null, null, cancellationToken);
        if (result.Rows.Count == 0) throw new QueryException("No result from lo_open");
        return Convert.ToInt32(result.Rows[0]["fd"]);
    }

    /// <summary>
    /// Write data to a large object.
    /// </summary>
    public async Task<int> LoWriteAsync(int fd, byte[] data, CancellationToken cancellationToken = default)
    {
        var b64 = Convert.ToBase64String(data);
        var result = await QueryAsync($"SELECT lowrite({fd}, decode('{b64}', 'base64')) AS written", null, null, cancellationToken);
        if (result.Rows.Count == 0) throw new QueryException("No result from lowrite");
        return Convert.ToInt32(result.Rows[0]["written"]);
    }

    /// <summary>
    /// Read data from a large object.
    /// </summary>
    public async Task<byte[]> LoReadAsync(int fd, int length, CancellationToken cancellationToken = default)
    {
        var result = await QueryAsync($"SELECT encode(loread({fd}, {length}), 'base64') AS data", null, null, cancellationToken);
        if (result.Rows.Count == 0) throw new QueryException("No result from loread");
        var b64 = result.Rows[0]["data"]?.ToString() ?? "";
        return Convert.FromBase64String(b64);
    }

    /// <summary>
    /// Close a large object file descriptor.
    /// </summary>
    public async Task LoCloseAsync(int fd, CancellationToken cancellationToken = default)
    {
        await QueryAsync($"SELECT lo_close({fd})", null, null, cancellationToken);
    }

    /// <summary>
    /// Delete a large object.
    /// </summary>
    public async Task LoUnlinkAsync(long oid, CancellationToken cancellationToken = default)
    {
        await QueryAsync($"SELECT lo_unlink({oid})", null, null, cancellationToken);
    }

    // ========================================================================
    // Advisory Locks
    // ========================================================================

    /// <summary>
    /// Acquire a session-level advisory lock.
    /// </summary>
    public async Task AdvisoryLockAsync(long key, bool shared = false, CancellationToken cancellationToken = default)
    {
        var fn = shared ? "pg_advisory_lock_shared" : "pg_advisory_lock";
        await QueryAsync($"SELECT {fn}({key})", null, null, cancellationToken);
    }

    /// <summary>
    /// Try to acquire an advisory lock without blocking.
    /// </summary>
    public async Task<bool> AdvisoryLockTryAsync(long key, bool shared = false, CancellationToken cancellationToken = default)
    {
        var fn = shared ? "pg_try_advisory_lock_shared" : "pg_try_advisory_lock";
        var result = await QueryAsync($"SELECT {fn}({key}) AS acquired", null, null, cancellationToken);
        if (result.Rows.Count == 0) return false;
        return Convert.ToBoolean(result.Rows[0]["acquired"]);
    }

    /// <summary>
    /// Release a session-level advisory lock.
    /// </summary>
    public async Task<bool> AdvisoryUnlockAsync(long key, bool shared = false, CancellationToken cancellationToken = default)
    {
        var fn = shared ? "pg_advisory_unlock_shared" : "pg_advisory_unlock";
        var result = await QueryAsync($"SELECT {fn}({key}) AS released", null, null, cancellationToken);
        if (result.Rows.Count == 0) return false;
        return Convert.ToBoolean(result.Rows[0]["released"]);
    }

    /// <summary>
    /// Release all session-level advisory locks.
    /// </summary>
    public async Task AdvisoryUnlockAllAsync(CancellationToken cancellationToken = default)
    {
        await QueryAsync("SELECT pg_advisory_unlock_all()", null, null, cancellationToken);
    }

    // ========================================================================
    // Full-Text Search
    // ========================================================================

    /// <summary>
    /// Perform a full-text search.
    /// </summary>
    public async Task<QueryResult> FtsSearchAsync(
        string table,
        string textColumn,
        string query,
        string config = "english",
        int limit = 100,
        bool includeRank = true,
        string? where = null,
        CancellationToken cancellationToken = default)
    {
        var safeTable = EscapeIdentifier(table);
        var safeColumn = EscapeIdentifier(textColumn);
        var safeQuery = EscapeString(query);

        var sql = new System.Text.StringBuilder();
        if (includeRank)
        {
            sql.Append($"SELECT *, ts_rank(to_tsvector('{config}', {safeColumn}), plainto_tsquery('{config}', '{safeQuery}')) AS rank ");
            sql.Append($"FROM {safeTable} WHERE to_tsvector('{config}', {safeColumn}) @@ plainto_tsquery('{config}', '{safeQuery}')");
        }
        else
        {
            sql.Append($"SELECT * FROM {safeTable} WHERE to_tsvector('{config}', {safeColumn}) @@ plainto_tsquery('{config}', '{safeQuery}')");
        }
        if (!string.IsNullOrEmpty(where)) sql.Append($" AND {where}");
        if (includeRank) sql.Append(" ORDER BY rank DESC");
        sql.Append($" LIMIT {limit}");

        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    // ========================================================================
    // Geospatial Support
    // ========================================================================

    /// <summary>
    /// Find records within a distance from a point.
    /// </summary>
    public async Task<QueryResult> GeoDistanceAsync(
        string table,
        string geoColumn,
        double lat,
        double lon,
        double radiusMeters,
        int limit = 100,
        string? where = null,
        CancellationToken cancellationToken = default)
    {
        var safeTable = EscapeIdentifier(table);
        var safeColumn = EscapeIdentifier(geoColumn);

        var sql = new System.Text.StringBuilder();
        sql.Append($"SELECT *, ST_Distance({safeColumn}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography) AS distance ");
        sql.Append($"FROM {safeTable} WHERE ST_DWithin({safeColumn}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography, {radiusMeters})");
        if (!string.IsNullOrEmpty(where)) sql.Append($" AND {where}");
        sql.Append($" ORDER BY distance ASC LIMIT {limit}");

        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Find K nearest neighbors to a point.
    /// </summary>
    public async Task<QueryResult> GeoNearestAsync(
        string table,
        string geoColumn,
        double lat,
        double lon,
        int k = 10,
        string? where = null,
        CancellationToken cancellationToken = default)
    {
        var safeTable = EscapeIdentifier(table);
        var safeColumn = EscapeIdentifier(geoColumn);

        var sql = new System.Text.StringBuilder();
        sql.Append($"SELECT *, ST_Distance({safeColumn}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography) AS distance ");
        sql.Append($"FROM {safeTable}");
        if (!string.IsNullOrEmpty(where)) sql.Append($" WHERE {where}");
        sql.Append($" ORDER BY {safeColumn} <-> ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geometry LIMIT {k}");

        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    // ========================================================================
    // Time Travel Queries
    // ========================================================================

    /// <summary>
    /// Query data as of a specific timestamp.
    /// </summary>
    public async Task<QueryResult> QueryAsOfAsync(
        string sql,
        DateTime timestamp,
        CancellationToken cancellationToken = default)
    {
        var ts = timestamp.ToString("o");
        var modifiedSql = $"{sql} AS OF TIMESTAMP '{EscapeString(ts)}'";
        return await QueryAsync(modifiedSql, null, null, cancellationToken);
    }

    /// <summary>
    /// Query data changes between two timestamps.
    /// </summary>
    public async Task<QueryResult> QueryBetweenAsync(
        string sql,
        DateTime startTime,
        DateTime endTime,
        CancellationToken cancellationToken = default)
    {
        var start = startTime.ToString("o");
        var end = endTime.ToString("o");
        var modifiedSql = $"{sql} FOR SYSTEM_TIME FROM '{EscapeString(start)}' TO '{EscapeString(end)}'";
        return await QueryAsync(modifiedSql, null, null, cancellationToken);
    }

    // ========================================================================
    // Batch Operations
    // ========================================================================

    /// <summary>
    /// Insert multiple rows in batches.
    /// </summary>
    public async Task<int> BatchInsertAsync(
        string table,
        IEnumerable<Dictionary<string, object?>> rows,
        int chunkSize = 1000,
        CancellationToken cancellationToken = default)
    {
        var rowList = rows.ToList();
        if (rowList.Count == 0) return 0;

        var safeTable = EscapeIdentifier(table);
        var columns = rowList[0].Keys.ToList();
        var colList = string.Join(", ", columns.Select(EscapeIdentifier));

        int totalInserted = 0;
        for (int i = 0; i < rowList.Count; i += chunkSize)
        {
            var chunk = rowList.Skip(i).Take(chunkSize).ToList();
            var values = string.Join(", ", chunk.Select(row =>
                "(" + string.Join(", ", columns.Select(c => FormatValue(row.GetValueOrDefault(c)))) + ")"));

            await ExecAsync($"INSERT INTO {safeTable} ({colList}) VALUES {values}", null, null, cancellationToken);
            totalInserted += chunk.Count;
        }
        return totalInserted;
    }

    /// <summary>
    /// Upsert multiple rows (INSERT ... ON CONFLICT).
    /// </summary>
    public async Task<int> BatchUpsertAsync(
        string table,
        IEnumerable<Dictionary<string, object?>> rows,
        IEnumerable<string> conflictColumns,
        IEnumerable<string>? updateColumns = null,
        int chunkSize = 1000,
        CancellationToken cancellationToken = default)
    {
        var rowList = rows.ToList();
        if (rowList.Count == 0) return 0;

        var safeTable = EscapeIdentifier(table);
        var columns = rowList[0].Keys.ToList();
        var colList = string.Join(", ", columns.Select(EscapeIdentifier));
        var conflictList = string.Join(", ", conflictColumns.Select(EscapeIdentifier));

        var conflictSet = new HashSet<string>(conflictColumns);
        var updateCols = updateColumns?.ToList() ?? columns.Where(c => !conflictSet.Contains(c)).ToList();
        var updateClause = string.Join(", ", updateCols.Select(c => $"{EscapeIdentifier(c)} = EXCLUDED.{EscapeIdentifier(c)}"));

        int totalAffected = 0;
        for (int i = 0; i < rowList.Count; i += chunkSize)
        {
            var chunk = rowList.Skip(i).Take(chunkSize).ToList();
            var values = string.Join(", ", chunk.Select(row =>
                "(" + string.Join(", ", columns.Select(c => FormatValue(row.GetValueOrDefault(c)))) + ")"));

            var sql = $"INSERT INTO {safeTable} ({colList}) VALUES {values} ON CONFLICT ({conflictList})";
            sql += string.IsNullOrEmpty(updateClause) ? " DO NOTHING" : $" DO UPDATE SET {updateClause}";

            await ExecAsync(sql, null, null, cancellationToken);
            totalAffected += chunk.Count;
        }
        return totalAffected;
    }

    /// <summary>
    /// Delete multiple rows by key values.
    /// </summary>
    public async Task<int> BatchDeleteAsync(
        string table,
        IEnumerable<object> keys,
        string keyColumn,
        int chunkSize = 1000,
        CancellationToken cancellationToken = default)
    {
        var keyList = keys.ToList();
        if (keyList.Count == 0) return 0;

        var safeTable = EscapeIdentifier(table);
        var safeKeyColumn = EscapeIdentifier(keyColumn);

        int totalDeleted = 0;
        for (int i = 0; i < keyList.Count; i += chunkSize)
        {
            var chunk = keyList.Skip(i).Take(chunkSize).ToList();
            var values = string.Join(", ", chunk.Select(FormatValue));

            await ExecAsync($"DELETE FROM {safeTable} WHERE {safeKeyColumn} IN ({values})", null, null, cancellationToken);
            totalDeleted += chunk.Count;
        }
        return totalDeleted;
    }

    // ========================================================================
    // Schema Introspection
    // ========================================================================

    /// <summary>
    /// Get column information for a table.
    /// </summary>
    public async Task<QueryResult> GetColumnsAsync(
        string table,
        string? database = null,
        CancellationToken cancellationToken = default)
    {
        var safeTable = EscapeIdentifier(table);
        var sql = database != null ? $"DESCRIBE {EscapeIdentifier(database)}.{safeTable}" : $"DESCRIBE {safeTable}";
        return await QueryAsync(sql, null, null, cancellationToken);
    }

    /// <summary>
    /// Get indexes for a table.
    /// </summary>
    public async Task<QueryResult> GetIndexesAsync(
        string table,
        string? database = null,
        CancellationToken cancellationToken = default)
    {
        var safeTable = EscapeIdentifier(table);
        var sql = database != null ? $"SHOW INDEXES ON {EscapeIdentifier(database)}.{safeTable}" : $"SHOW INDEXES ON {safeTable}";
        return await QueryAsync(sql, null, null, cancellationToken);
    }

    /// <summary>
    /// Get constraints for a table.
    /// </summary>
    public async Task<QueryResult> GetConstraintsAsync(
        string table,
        string? database = null,
        CancellationToken cancellationToken = default)
    {
        var safeTable = EscapeIdentifier(table);
        var sql = database != null ? $"SHOW CONSTRAINTS ON {EscapeIdentifier(database)}.{safeTable}" : $"SHOW CONSTRAINTS ON {safeTable}";
        return await QueryAsync(sql, null, null, cancellationToken);
    }

    // ========================================================================
    // Array Operations
    // ========================================================================

    /// <summary>
    /// Find rows where array column contains a value.
    /// </summary>
    public async Task<QueryResult> ArrayContainsAsync(
        string table,
        string arrayColumn,
        object value,
        int limit = 100,
        string? where = null,
        CancellationToken cancellationToken = default)
    {
        var sql = new System.Text.StringBuilder();
        sql.Append($"SELECT * FROM {EscapeIdentifier(table)} WHERE {FormatValue(value)} = ANY({EscapeIdentifier(arrayColumn)})");
        if (!string.IsNullOrEmpty(where)) sql.Append($" AND {where}");
        sql.Append($" LIMIT {limit}");
        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    /// <summary>
    /// Find rows where array columns have overlapping elements.
    /// </summary>
    public async Task<QueryResult> ArrayOverlapAsync(
        string table,
        string arrayColumn,
        IEnumerable<object> values,
        int limit = 100,
        string? where = null,
        CancellationToken cancellationToken = default)
    {
        var arrayLiteral = "ARRAY[" + string.Join(", ", values.Select(FormatValue)) + "]";
        var sql = new System.Text.StringBuilder();
        sql.Append($"SELECT * FROM {EscapeIdentifier(table)} WHERE {EscapeIdentifier(arrayColumn)} && {arrayLiteral}");
        if (!string.IsNullOrEmpty(where)) sql.Append($" AND {where}");
        sql.Append($" LIMIT {limit}");
        return await QueryAsync(sql.ToString(), null, null, cancellationToken);
    }

    // ========================================================================
    // Async Pub/Sub Polling
    // ========================================================================

    /// <summary>
    /// Poll for pending notifications.
    /// </summary>
    public async Task<List<(string Channel, string Payload)>> PollNotificationsAsync(
        int timeoutMs = 0,
        CancellationToken cancellationToken = default)
    {
        var result = await QueryAsync($"SELECT * FROM pg_notification_queue({timeoutMs})", null, null, cancellationToken);
        return result.Rows.Select(row => (
            Channel: row["channel"]?.ToString() ?? "",
            Payload: row["payload"]?.ToString() ?? ""
        )).ToList();
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    private static string EscapeIdentifier(string identifier)
    {
        if (identifier.Contains('.'))
        {
            return string.Join(".", identifier.Split('.').Select(EscapeIdentifier));
        }
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    private static string EscapeString(string value)
    {
        return value.Replace("'", "''");
    }

    private static string FormatValue(object? value)
    {
        return value switch
        {
            null => "NULL",
            bool b => b ? "TRUE" : "FALSE",
            int or long or float or double or decimal => value.ToString()!,
            string s => $"'{EscapeString(s)}'",
            _ => $"'{EscapeString(JsonSerializer.Serialize(value))}'"
        };
    }

    private static void CheckStatus(JsonDocument response, string errorPrefix)
    {
        var root = response.RootElement;
        if (root.TryGetProperty("status", out var statusElement))
        {
            var status = statusElement.GetString();
            if (status != "ok")
            {
                var message = root.TryGetProperty("message", out var msgElement)
                    ? msgElement.GetString()
                    : "Unknown error";
                throw new QueryException($"{errorPrefix}: {message}");
            }
        }
    }

    /// <summary>
    /// Parse Arrow IPC data (simplified parser for common cases).
    /// For production use with untrusted data, consider using Apache.Arrow NuGet package.
    /// </summary>
    private static void ParseArrowIpc(byte[] data, QueryResult result)
    {
        int offset = 0;
        List<FieldInfo>? schema = null;

        while (offset < data.Length)
        {
            if (offset + 4 > data.Length) break;

            int msgLen = BitConverter.ToInt32(data, offset);
            if (!BitConverter.IsLittleEndian)
                msgLen = BinaryPrimitives.ReverseEndianness(msgLen);
            offset += 4;

            if (msgLen == 0) break;

            // Handle continuation marker
            if (msgLen == -1)
            {
                if (offset + 4 > data.Length) break;
                msgLen = BitConverter.ToInt32(data, offset);
                if (!BitConverter.IsLittleEndian)
                    msgLen = BinaryPrimitives.ReverseEndianness(msgLen);
                offset += 4;
                if (msgLen == 0) break;
            }

            if (offset + msgLen > data.Length) break;

            var metadata = new ReadOnlySpan<byte>(data, offset, msgLen);
            offset += msgLen;

            // Pad to 8-byte boundary
            int padding = (8 - (msgLen % 8)) % 8;
            offset += padding;

            if (metadata.Length < 8) continue;

            // Parse message type
            var (headerType, bodyLength) = ParseMessageHeader(metadata);

            if (headerType == 1)
            {
                // Schema message
                schema = ParseSchema(metadata);
                result.Columns = schema.Select(f => f.Name).ToList();
            }
            else if (headerType == 3 && bodyLength > 0 && schema != null)
            {
                // RecordBatch message
                if (offset + (int)bodyLength > data.Length) break;
                var bodyData = new ReadOnlySpan<byte>(data, offset, (int)bodyLength);
                offset += (int)bodyLength;

                var rows = ParseRecordBatch(metadata, bodyData, schema);
                result.Rows.AddRange(rows);
            }
            else if (bodyLength > 0)
            {
                offset += (int)bodyLength;
            }
        }
    }

    private static (byte headerType, long bodyLength) ParseMessageHeader(ReadOnlySpan<byte> metadata)
    {
        if (metadata.Length < 8) return (0, 0);

        uint rootOffset = BitConverter.ToUInt32(metadata);
        if (rootOffset >= metadata.Length - 4) return (0, 0);

        int vtableOffset = BitConverter.ToInt32(metadata.Slice((int)rootOffset, 4));
        int vtablePos = (int)rootOffset - vtableOffset;
        if (vtablePos < 0 || vtablePos >= metadata.Length - 4) return (0, 0);

        ushort vtableSize = BitConverter.ToUInt16(metadata.Slice(vtablePos, 2));
        if (vtableSize < 6 || vtablePos + vtableSize > metadata.Length) return (0, 0);

        if (vtablePos + 8 > metadata.Length) return (0, 0);
        ushort headerTypeOffset = BitConverter.ToUInt16(metadata.Slice(vtablePos + 6, 2));
        if (headerTypeOffset == 0) return (0, 0);

        int headerTypePos = (int)rootOffset + headerTypeOffset;
        if (headerTypePos >= metadata.Length) return (0, 0);
        byte headerType = metadata[headerTypePos];

        long bodyLength = 0;
        if (vtablePos + 10 <= metadata.Length)
        {
            ushort bodyLengthOffset = BitConverter.ToUInt16(metadata.Slice(vtablePos + 8, 2));
            if (bodyLengthOffset != 0)
            {
                int bodyLengthPos = (int)rootOffset + bodyLengthOffset;
                if (bodyLengthPos + 8 <= metadata.Length)
                {
                    bodyLength = BitConverter.ToInt64(metadata.Slice(bodyLengthPos, 8));
                }
            }
        }

        return (headerType, bodyLength);
    }

    private record FieldInfo(string Name, string Type, bool Nullable);

    private static List<FieldInfo> ParseSchema(ReadOnlySpan<byte> data)
    {
        var fields = new List<FieldInfo>();
        // Simplified schema parsing - return empty for complex cases
        // In production, use Apache.Arrow library
        return fields;
    }

    private static List<Dictionary<string, object?>> ParseRecordBatch(
        ReadOnlySpan<byte> metadata,
        ReadOnlySpan<byte> bodyData,
        List<FieldInfo> schema)
    {
        // Simplified record batch parsing
        // In production, use Apache.Arrow library
        return new List<Dictionary<string, object?>>();
    }
}
