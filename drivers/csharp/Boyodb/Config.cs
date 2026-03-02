namespace Boyodb;

/// <summary>
/// Configuration options for the boyodb client.
/// </summary>
public class Config
{
    /// <summary>
    /// Enable TLS encryption.
    /// </summary>
    public bool Tls { get; set; } = false;

    /// <summary>
    /// Path to CA certificate file for TLS verification.
    /// </summary>
    public string? CaFile { get; set; }

    /// <summary>
    /// Skip TLS certificate verification.
    /// WARNING: SECURITY RISK - Disables certificate validation, making connections
    /// vulnerable to man-in-the-middle (MITM) attacks. NEVER use in production.
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
    /// Maximum connection retry attempts.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Delay between retries.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Default database for queries.
    /// </summary>
    public string? Database { get; set; }

    /// <summary>
    /// Default query timeout in milliseconds.
    /// </summary>
    public int QueryTimeout { get; set; } = 30000;
}
