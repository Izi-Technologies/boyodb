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

For high-performance concurrent access, use the pooled client:
    from boyodb import PooledClient, PoolConfig

    config = PoolConfig(host="localhost", port=8765, pool_size=20)
    client = PooledClient(config)

    result = client.query("SELECT COUNT(*) FROM events")
    client.close()
"""

from .client import Client, Config, QueryResult, TableInfo
from .pool import ConnectionPool, PoolConfig, PooledClient
from .errors import (
    BoyodbError,
    ConnectionError,
    AuthError,
    QueryError,
    TimeoutError,
)

__version__ = "0.1.0"
__all__ = [
    # Single connection client
    "Client",
    "Config",
    "QueryResult",
    "TableInfo",
    # Connection pooling
    "ConnectionPool",
    "PoolConfig",
    "PooledClient",
    # Errors
    "BoyodbError",
    "ConnectionError",
    "AuthError",
    "QueryError",
    "TimeoutError",
]
