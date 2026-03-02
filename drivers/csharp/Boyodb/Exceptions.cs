namespace Boyodb;

/// <summary>
/// Base exception for boyodb errors.
/// </summary>
public class BoyodbException : Exception
{
    public BoyodbException(string message) : base(message) { }
    public BoyodbException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// Thrown when connection to the server fails.
/// </summary>
public class ConnectionException : BoyodbException
{
    public ConnectionException(string message) : base(message) { }
    public ConnectionException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// Thrown when a query fails.
/// </summary>
public class QueryException : BoyodbException
{
    public QueryException(string message) : base(message) { }
}

/// <summary>
/// Thrown when authentication fails.
/// </summary>
public class AuthException : BoyodbException
{
    public AuthException(string message) : base(message) { }
}

/// <summary>
/// Thrown when an operation times out.
/// </summary>
public class TimeoutException : BoyodbException
{
    public TimeoutException(string message) : base(message) { }
}
