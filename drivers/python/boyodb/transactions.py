"""
Transaction support for BoyoDB.

Provides ACID transaction support with savepoints and isolation levels.
"""

from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from .client import Client


class IsolationLevel(Enum):
    """Transaction isolation levels."""
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"
    SNAPSHOT = "SNAPSHOT"


class Transaction:
    """
    Transaction context manager for ACID operations.

    Usage:
        with client.transaction() as tx:
            tx.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            tx.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
            # Automatically commits on success, rolls back on exception

    With savepoints:
        with client.transaction() as tx:
            tx.execute("INSERT INTO orders (id) VALUES (1)")
            tx.savepoint("sp1")
            try:
                tx.execute("INSERT INTO order_items (order_id) VALUES (1)")
            except:
                tx.rollback_to("sp1")
            # Transaction continues
    """

    def __init__(
        self,
        client: "Client",
        isolation_level: Optional[IsolationLevel] = None,
        read_only: bool = False,
    ):
        self._client = client
        self._isolation_level = isolation_level
        self._read_only = read_only
        self._active = False
        self._savepoints: List[str] = []

    def __enter__(self) -> "Transaction":
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()

    def begin(self) -> None:
        """Start the transaction."""
        sql = "BEGIN"
        if self._isolation_level:
            sql += f" ISOLATION LEVEL {self._isolation_level.value}"
        if self._read_only:
            sql += " READ ONLY"

        self._client.exec(sql)
        self._active = True

    def commit(self) -> None:
        """Commit the transaction."""
        if self._active:
            self._client.exec("COMMIT")
            self._active = False
            self._savepoints.clear()

    def rollback(self) -> None:
        """Rollback the transaction."""
        if self._active:
            self._client.exec("ROLLBACK")
            self._active = False
            self._savepoints.clear()

    def savepoint(self, name: str) -> None:
        """Create a savepoint."""
        if not self._active:
            raise RuntimeError("No active transaction")
        self._client.exec(f"SAVEPOINT {name}")
        self._savepoints.append(name)

    def rollback_to(self, name: str) -> None:
        """Rollback to a savepoint."""
        if not self._active:
            raise RuntimeError("No active transaction")
        self._client.exec(f"ROLLBACK TO SAVEPOINT {name}")
        # Remove savepoints after the one we rolled back to
        if name in self._savepoints:
            idx = self._savepoints.index(name)
            self._savepoints = self._savepoints[:idx + 1]

    def release(self, name: str) -> None:
        """Release a savepoint."""
        if not self._active:
            raise RuntimeError("No active transaction")
        self._client.exec(f"RELEASE SAVEPOINT {name}")
        if name in self._savepoints:
            self._savepoints.remove(name)

    def execute(self, sql: str, database: Optional[str] = None) -> Any:
        """Execute SQL within the transaction."""
        if not self._active:
            raise RuntimeError("No active transaction")
        return self._client.query(sql, database)

    @property
    def is_active(self) -> bool:
        """Check if transaction is active."""
        return self._active
