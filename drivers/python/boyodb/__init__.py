"""
boyodb Python Driver

A comprehensive Python client for connecting to boyodb-server with support for:
- Basic queries and transactions
- Connection pooling for concurrent access
- Async inserts for high-throughput ingestion
- Pub/Sub and CDC subscriptions
- Approximate analytics (HyperLogLog, T-Digest)
- Time series and graph analytics
- External tables (S3, URL, HDFS)
- Vector similarity search

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

For transactions:
    with client.transaction() as tx:
        tx.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        tx.execute("UPDATE accounts SET balance = balance - 100")
        # Auto-commits on success, rolls back on exception

For async inserts:
    from boyodb import AsyncInsertBuffer, AsyncInsertConfig

    buffer = AsyncInsertBuffer(client, "mydb.events", AsyncInsertConfig(max_rows=10000))
    buffer.start()
    buffer.insert({"id": 1, "event": "click"})
    buffer.stop()
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
from .transactions import Transaction, IsolationLevel
from .async_insert import AsyncInsertBuffer, AsyncInsertConfig
from .subscriptions import (
    Subscriber,
    CDCSubscriber,
    Publisher,
    Message,
    ChangeEvent,
    ChangeType,
)
from .analytics import (
    ApproximateFunctions,
    TimeSeriesAnalytics,
    GraphAnalytics,
    CardinalityEstimate,
    QuantileEstimate,
    FrequencyEstimate,
)
from .external import (
    ExternalTables,
    VectorSearch,
    S3Config,
)

__version__ = "0.9.8"
__all__ = [
    # Core client
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
    # Transactions
    "Transaction",
    "IsolationLevel",
    # Async inserts
    "AsyncInsertBuffer",
    "AsyncInsertConfig",
    # Pub/Sub and CDC
    "Subscriber",
    "CDCSubscriber",
    "Publisher",
    "Message",
    "ChangeEvent",
    "ChangeType",
    # Analytics
    "ApproximateFunctions",
    "TimeSeriesAnalytics",
    "GraphAnalytics",
    "CardinalityEstimate",
    "QuantileEstimate",
    "FrequencyEstimate",
    # External data
    "ExternalTables",
    "VectorSearch",
    "S3Config",
]


# Add transaction() method to Client
def _client_transaction(
    self,
    isolation_level: IsolationLevel = None,
    read_only: bool = False,
) -> Transaction:
    """
    Create a transaction context manager.

    Args:
        isolation_level: Transaction isolation level
        read_only: Whether transaction is read-only

    Returns:
        Transaction context manager

    Example:
        with client.transaction() as tx:
            tx.execute("INSERT INTO users VALUES (1, 'Alice')")
            tx.savepoint("sp1")
            tx.execute("INSERT INTO orders VALUES (1, 1)")
    """
    return Transaction(self, isolation_level, read_only)


def _client_approximate(self) -> ApproximateFunctions:
    """Get approximate analytics functions."""
    return ApproximateFunctions(self)


def _client_timeseries(self) -> TimeSeriesAnalytics:
    """Get time series analytics functions."""
    return TimeSeriesAnalytics(self)


def _client_graph(self) -> GraphAnalytics:
    """Get graph analytics functions."""
    return GraphAnalytics(self)


def _client_external(self) -> ExternalTables:
    """Get external tables helper."""
    return ExternalTables(self)


def _client_vector(self) -> VectorSearch:
    """Get vector search helper."""
    return VectorSearch(self)


def _client_publisher(self) -> Publisher:
    """Get pub/sub publisher."""
    return Publisher(self)


# Attach methods to Client class
Client.transaction = _client_transaction
Client.approximate = property(_client_approximate)
Client.timeseries = property(_client_timeseries)
Client.graph = property(_client_graph)
Client.external = property(_client_external)
Client.vector = property(_client_vector)
Client.publisher = property(_client_publisher)
