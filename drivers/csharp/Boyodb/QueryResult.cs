namespace Boyodb;

/// <summary>
/// Result of a query execution.
/// </summary>
public class QueryResult
{
    /// <summary>
    /// List of rows as dictionaries.
    /// </summary>
    public List<Dictionary<string, object?>> Rows { get; set; } = new();

    /// <summary>
    /// Column names.
    /// </summary>
    public List<string> Columns { get; set; } = new();

    /// <summary>
    /// Number of rows in the result.
    /// </summary>
    public int RowCount => Rows.Count;

    /// <summary>
    /// Number of segments scanned.
    /// </summary>
    public int SegmentsScanned { get; set; }

    /// <summary>
    /// Bytes skipped due to pruning.
    /// </summary>
    public long DataSkippedBytes { get; set; }
}

/// <summary>
/// Table metadata.
/// </summary>
public class TableInfo
{
    /// <summary>
    /// Database name.
    /// </summary>
    public string Database { get; set; } = string.Empty;

    /// <summary>
    /// Table name.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Schema JSON (optional).
    /// </summary>
    public string? SchemaJson { get; set; }
}
