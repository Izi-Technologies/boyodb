"""
boyodb Python Driver

A Python client for connecting to boyodb-server.

Example usage:
    from boyodb import Client

    client = Client("localhost:8765")
    client.connect()

    result = client.query("SELECT * FROM users LIMIT 10")
    for row in result:
        print(row)

    client.close()
"""

from .client import Client, Config, QueryResult, TableInfo
from .errors import (
    BoyodbError,
    ConnectionError,
    AuthError,
    QueryError,
    TimeoutError,
)

__version__ = "0.1.0"
__all__ = [
    "Client",
    "Config",
    "QueryResult",
    "TableInfo",
    "BoyodbError",
    "ConnectionError",
    "AuthError",
    "QueryError",
    "TimeoutError",
]
