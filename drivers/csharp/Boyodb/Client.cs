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
