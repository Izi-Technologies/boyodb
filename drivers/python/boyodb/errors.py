"""
boyodb error types.
"""


class BoyodbError(Exception):
    """Base exception for boyodb errors."""

    def __init__(self, message: str, operation: str = None):
        self.message = message
        self.operation = operation
        super().__init__(message)


class ConnectionError(BoyodbError):
    """Raised when connection fails."""

    pass


class AuthError(BoyodbError):
    """Raised when authentication fails."""

    pass


class QueryError(BoyodbError):
    """Raised when a query fails."""

    pass


class TimeoutError(BoyodbError):
    """Raised when an operation times out."""

    pass
