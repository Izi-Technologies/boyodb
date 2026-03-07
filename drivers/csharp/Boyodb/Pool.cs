using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;

namespace Boyodb;

/// <summary>
/// Configuration for connection pool.
/// </summary>
public class PoolConfig
{
    /// <summary>
    /// Server host.
    /// </summary>
    public string Host { get; set; } = "localhost";

    /// <summary>
    /// Server port.
    /// </summary>
    public int Port { get; set; } = 8765;

    /// <summary>
    /// Number of connections in the pool.
    /// </summary>
    public int PoolSize { get; set; } = 10;

    /// <summary>
    /// Timeout for acquiring a connection from the pool.
    /// </summary>
    public TimeSpan PoolTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Enable TLS encryption.
    /// </summary>
    public bool Tls { get; set; } = false;

    /// <summary>
    /// Path to CA certificate file.
    /// </summary>
    public string? CaFile { get; set; }

    /// <summary>
    /// Skip TLS verification (DANGEROUS - development only).
    /// </summary>
    public bool InsecureSkipVerify { get; set; } = false;

    /// <summary>
    /// Connection timeout.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Read timeout.
    /// </summary>
    public TimeSpan ReadTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Write timeout.
    /// </summary>
    public TimeSpan WriteTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Authentication token.
    /// </summary>
    public string? Token { get; set; }

    /// <summary>
    /// Max connection retries.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Retry delay.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Default database.
    /// </summary>
    public string? Database { get; set; }

    /// <summary>
    /// Query timeout in milliseconds.
    /// </summary>
    public int QueryTimeout { get; set; } = 30000;
}

/// <summary>
/// A pooled connection wrapper.
/// </summary>
internal class PooledConnection : IDisposable
{
    private TcpClient? _tcpClient;
    private Stream? _stream;
    private bool _valid = true;
    private readonly DateTime _createdAt;

    public PooledConnection(TcpClient tcpClient, Stream stream)
    {
        _tcpClient = tcpClient;
        _stream = stream;
        _createdAt = DateTime.UtcNow;
    }

    public Stream? Stream => _stream;
    public bool IsValid => _valid && _tcpClient?.Connected == true;
    public DateTime CreatedAt => _createdAt;

    public void Invalidate() => _valid = false;

    public void Dispose()
    {
        _valid = false;
        _stream?.Dispose();
        _tcpClient?.Dispose();
        _stream = null;
        _tcpClient = null;
    }
}

/// <summary>
/// Thread-safe connection pool for BoyoDB.
/// </summary>
public class ConnectionPool : IDisposable
{
    private readonly PoolConfig _config;
    private readonly BlockingCollection<PooledConnection> _pool;
    private readonly SemaphoreSlim _semaphore;
    private string? _sessionId;
    private bool _disposed;
    private readonly object _lock = new();

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// Create a new connection pool.
    /// </summary>
    public ConnectionPool(PoolConfig config)
    {
        _config = config;
        _pool = new BlockingCollection<PooledConnection>(_config.PoolSize);
        _semaphore = new SemaphoreSlim(_config.PoolSize, _config.PoolSize);
    }

    /// <summary>
    /// Initialize the pool with connections.
    /// </summary>
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var tasks = new List<Task>();
        for (int i = 0; i < _config.PoolSize; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                var conn = await CreateConnectionAsync(cancellationToken);
                _pool.Add(conn);
            }, cancellationToken));
        }
        await Task.WhenAll(tasks);

        // Health check
        await HealthAsync(cancellationToken);
    }

    private async Task<PooledConnection> CreateConnectionAsync(CancellationToken cancellationToken)
    {
        var tcpClient = new TcpClient();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_config.ConnectTimeout);

        await tcpClient.ConnectAsync(_config.Host, _config.Port, cts.Token);

        Stream stream = tcpClient.GetStream();

        if (_config.Tls)
        {
            var sslStream = new SslStream(stream, leaveInnerStreamOpen: false, ValidateServerCertificate);

            if (_config.InsecureSkipVerify)
            {
                Console.Error.WriteLine("WARNING: TLS certificate verification is DISABLED.");
            }

            var sslOptions = new SslClientAuthenticationOptions
            {
                TargetHost = _config.Host,
            };

            await sslStream.AuthenticateAsClientAsync(sslOptions, cts.Token);
            stream = sslStream;
        }

        return new PooledConnection(tcpClient, stream);
    }

    private bool ValidateServerCertificate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors sslPolicyErrors)
    {
        if (_config.InsecureSkipVerify) return true;
        if (sslPolicyErrors == SslPolicyErrors.None) return true;

        if (!string.IsNullOrEmpty(_config.CaFile) && File.Exists(_config.CaFile))
        {
            var caCert = new X509Certificate2(_config.CaFile);
            chain?.ChainPolicy.ExtraStore.Add(caCert);
            chain?.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
            return chain?.Build(new X509Certificate2(certificate!)) ?? false;
        }

        return false;
    }

    private async Task<PooledConnection> BorrowAsync(CancellationToken cancellationToken)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(ConnectionPool));

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_config.PoolTimeout);

        if (_pool.TryTake(out var conn, (int)_config.PoolTimeout.TotalMilliseconds, cts.Token))
        {
            if (conn.IsValid) return conn;

            conn.Dispose();
            return await CreateConnectionAsync(cancellationToken);
        }

        throw new ConnectionException("Connection pool exhausted");
    }

    private void Return(PooledConnection conn)
    {
        if (conn.IsValid && !_disposed)
        {
            if (!_pool.TryAdd(conn))
            {
                conn.Dispose();
            }
        }
        else
        {
            conn.Dispose();
            if (!_disposed)
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        var newConn = await CreateConnectionAsync(CancellationToken.None);
                        if (!_pool.TryAdd(newConn))
                        {
                            newConn.Dispose();
                        }
                    }
                    catch { /* Ignore */ }
                });
            }
        }
    }

    internal async Task<JsonDocument> SendRequestAsync(
        Dictionary<string, object?> request,
        CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            if (!string.IsNullOrEmpty(_sessionId))
            {
                request["auth"] = _sessionId;
            }
            else if (!string.IsNullOrEmpty(_config.Token))
            {
                request["auth"] = _config.Token;
            }
        }

        var conn = await BorrowAsync(cancellationToken);
        try
        {
            var result = await SendOnConnectionAsync(conn, request, cancellationToken);
            Return(conn);
            return result;
        }
        catch
        {
            conn.Invalidate();
            Return(conn);
            throw;
        }
    }

    internal async Task<(JsonDocument Json, byte[]? IpcBytes)> SendRequestWithIpcAsync(
        Dictionary<string, object?> request,
        CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            if (!string.IsNullOrEmpty(_sessionId))
            {
                request["auth"] = _sessionId;
            }
            else if (!string.IsNullOrEmpty(_config.Token))
            {
                request["auth"] = _config.Token;
            }
        }

        var conn = await BorrowAsync(cancellationToken);
        try
        {
            var result = await SendOnConnectionWithIpcAsync(conn, request, cancellationToken);
            Return(conn);
            return result;
        }
        catch
        {
            conn.Invalidate();
            Return(conn);
            throw;
        }
    }

    private async Task<JsonDocument> SendOnConnectionAsync(
        PooledConnection conn,
        Dictionary<string, object?> request,
        CancellationToken cancellationToken)
    {
        var (json, _) = await SendOnConnectionWithIpcAsync(conn, request, cancellationToken);
        return json;
    }

    private async Task<(JsonDocument Json, byte[]? IpcBytes)> SendOnConnectionWithIpcAsync(
        PooledConnection conn,
        Dictionary<string, object?> request,
        CancellationToken cancellationToken)
    {
        var stream = conn.Stream ?? throw new ConnectionException("Connection closed");

        var json = JsonSerializer.Serialize(request, JsonOptions);
        var payload = Encoding.UTF8.GetBytes(json);

        var lengthBuffer = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(lengthBuffer, (uint)payload.Length);

        await stream.WriteAsync(lengthBuffer, cancellationToken);
        await stream.WriteAsync(payload, cancellationToken);
        await stream.FlushAsync(cancellationToken);

        var respLengthBuffer = new byte[4];
        await ReadExactAsync(stream, respLengthBuffer, cancellationToken);
        var respLength = BinaryPrimitives.ReadUInt32BigEndian(respLengthBuffer);

        if (respLength > 100 * 1024 * 1024)
        {
            throw new QueryException($"Response too large: {respLength} bytes");
        }

        var respBuffer = new byte[respLength];
        await ReadExactAsync(stream, respBuffer, cancellationToken);

        var jsonDoc = JsonDocument.Parse(respBuffer);
        byte[]? ipcBytes = null;

        if (jsonDoc.RootElement.TryGetProperty("ipc_streaming", out var ipcStreamingElement) &&
            ipcStreamingElement.ValueKind == JsonValueKind.True)
        {
            ipcBytes = await ReadStreamFramesAsync(stream, cancellationToken);
        }
        else if (jsonDoc.RootElement.TryGetProperty("ipc_len", out var ipcLenElement) &&
                 ipcLenElement.ValueKind == JsonValueKind.Number &&
                 ipcLenElement.TryGetInt64(out var ipcLen) &&
                 ipcLen > 0)
        {
            if (ipcLen > 100 * 1024 * 1024)
            {
                throw new QueryException($"Response too large: {ipcLen} bytes");
            }

            var ipcLenBuffer = new byte[4];
            await ReadExactAsync(stream, ipcLenBuffer, cancellationToken);
            var payloadLen = BinaryPrimitives.ReadUInt32BigEndian(ipcLenBuffer);
            if (payloadLen != ipcLen)
            {
                throw new QueryException($"IPC length mismatch");
            }

            ipcBytes = new byte[payloadLen];
            await ReadExactAsync(stream, ipcBytes, cancellationToken);
        }

        return (jsonDoc, ipcBytes);
    }

    private static async Task ReadExactAsync(Stream stream, byte[] buffer, CancellationToken ct)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset, buffer.Length - offset), ct);
            if (read == 0) throw new ConnectionException("Connection closed");
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
            if (chunkLen == 0) break;
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

        CheckStatus(response, "Health check failed");
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

        CheckStatus(response, "Login failed");

        if (response.RootElement.TryGetProperty("session_id", out var sessionIdElement))
        {
            lock (_lock)
            {
                _sessionId = sessionIdElement.GetString();
            }
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

        CheckStatus(response, "Logout failed");
        lock (_lock)
        {
            _sessionId = null;
        }
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
    /// Close the pool and all connections.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _pool.CompleteAdding();
        while (_pool.TryTake(out var conn))
        {
            conn.Dispose();
        }
        _pool.Dispose();
        _semaphore.Dispose();
    }
}

/// <summary>
/// High-performance pooled BoyoDB client.
/// </summary>
public class PooledClient : IDisposable
{
    private readonly PoolConfig _config;
    private readonly ConnectionPool _pool;
    private bool _initialized;

    /// <summary>
    /// Create a new pooled client.
    /// </summary>
    public PooledClient(PoolConfig config)
    {
        _config = config;
        _pool = new ConnectionPool(config);
    }

    /// <summary>
    /// Initialize the client and connection pool.
    /// </summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (_initialized) return;
        await _pool.InitializeAsync(cancellationToken);
        _initialized = true;
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
        if (!_initialized) await ConnectAsync(cancellationToken);

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

        var (response, ipcBytes) = await _pool.SendRequestWithIpcAsync(request, cancellationToken);
        using (response)
        {
            CheckStatus(response, "Query failed");

            var result = new QueryResult();

            if (response.RootElement.TryGetProperty("segments_scanned", out var segmentsElement))
            {
                result.SegmentsScanned = segmentsElement.GetInt32();
            }

            if (response.RootElement.TryGetProperty("data_skipped_bytes", out var skippedElement))
            {
                result.DataSkippedBytes = skippedElement.GetInt64();
            }

            if (ipcBytes != null)
            {
                ParseArrowIpc(ipcBytes, result);
            }
            else if (response.RootElement.TryGetProperty("ipc_base64", out var ipcElement))
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
    /// Login with username and password.
    /// </summary>
    public async Task LoginAsync(string username, string password, CancellationToken cancellationToken = default)
    {
        if (!_initialized) await ConnectAsync(cancellationToken);
        await _pool.LoginAsync(username, password, cancellationToken);
    }

    /// <summary>
    /// Logout from the server.
    /// </summary>
    public async Task LogoutAsync(CancellationToken cancellationToken = default)
    {
        await _pool.LogoutAsync(cancellationToken);
    }

    /// <summary>
    /// Set the default database.
    /// </summary>
    public void SetDatabase(string database) => _config.Database = database;

    /// <summary>
    /// Dispose the client.
    /// </summary>
    public void Dispose()
    {
        _pool.Dispose();
    }

    private static bool IsSelectLike(string sql)
    {
        var trimmed = sql.TrimStart().ToLowerInvariant();
        return trimmed.StartsWith("select ") || trimmed.StartsWith("with ");
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

    private static void ParseArrowIpc(byte[] data, QueryResult result)
    {
        // Simplified parsing - use Apache.Arrow NuGet for production
        // This is a placeholder that matches the Client.cs implementation
    }
}
