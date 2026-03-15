"""
Async Insert support for high-throughput data ingestion.

Buffers inserts locally and sends them in batches for optimal performance.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .client import Client


@dataclass
class AsyncInsertConfig:
    """Configuration for async inserts."""

    # Maximum rows to buffer before flush
    max_rows: int = 10000

    # Maximum bytes to buffer before flush
    max_bytes: int = 10 * 1024 * 1024  # 10MB

    # Maximum time to wait before flush (seconds)
    max_wait_seconds: float = 0.2

    # Enable deduplication within buffer
    deduplicate: bool = False

    # Deduplication key columns
    dedup_columns: List[str] = field(default_factory=list)

    # Number of retries on failure
    max_retries: int = 3

    # Callback on flush completion
    on_flush: Optional[Callable[[int, int], None]] = None

    # Callback on flush error
    on_error: Optional[Callable[[Exception], None]] = None


class AsyncInsertBuffer:
    """
    Buffer for async inserts with automatic flushing.

    Usage:
        buffer = AsyncInsertBuffer(client, "mydb.events", config)
        buffer.start()

        # Add rows - they'll be batched automatically
        buffer.insert({"id": 1, "event": "click"})
        buffer.insert({"id": 2, "event": "view"})

        # Flush remaining and stop
        buffer.stop()
    """

    def __init__(
        self,
        client: "Client",
        table: str,
        config: Optional[AsyncInsertConfig] = None,
    ):
        self._client = client
        self._table = table
        self._config = config or AsyncInsertConfig()

        self._buffer: List[Dict[str, Any]] = []
        self._buffer_bytes = 0
        self._lock = threading.Lock()
        self._running = False
        self._flush_thread: Optional[threading.Thread] = None

        # Statistics
        self._total_rows = 0
        self._total_flushes = 0
        self._total_errors = 0

        # Dedup set
        self._dedup_set: set = set() if self._config.deduplicate else None

    def start(self) -> None:
        """Start the background flush thread."""
        if self._running:
            return

        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

    def stop(self) -> None:
        """Stop the buffer and flush remaining rows."""
        self._running = False
        self.flush()
        if self._flush_thread:
            self._flush_thread.join(timeout=5.0)

    def insert(self, row: Dict[str, Any]) -> bool:
        """
        Add a row to the buffer.

        Returns True if row was added, False if deduplicated.
        """
        with self._lock:
            # Check deduplication
            if self._dedup_set is not None:
                key = self._make_dedup_key(row)
                if key in self._dedup_set:
                    return False
                self._dedup_set.add(key)

            # Estimate row size
            row_size = len(str(row))

            self._buffer.append(row)
            self._buffer_bytes += row_size

            # Check if immediate flush needed
            if (len(self._buffer) >= self._config.max_rows or
                self._buffer_bytes >= self._config.max_bytes):
                self._flush_internal()

            return True

    def insert_many(self, rows: List[Dict[str, Any]]) -> int:
        """
        Add multiple rows to the buffer.

        Returns the number of rows added.
        """
        count = 0
        for row in rows:
            if self.insert(row):
                count += 1
        return count

    def flush(self) -> None:
        """Manually flush the buffer."""
        with self._lock:
            self._flush_internal()

    def _flush_internal(self) -> None:
        """Internal flush (must hold lock)."""
        if not self._buffer:
            return

        rows = self._buffer
        self._buffer = []
        self._buffer_bytes = 0
        if self._dedup_set is not None:
            self._dedup_set.clear()

        # Build INSERT statement
        if not rows:
            return

        columns = list(rows[0].keys())
        col_str = ", ".join(columns)
        values_list = []

        for row in rows:
            vals = []
            for col in columns:
                val = row.get(col)
                if val is None:
                    vals.append("NULL")
                elif isinstance(val, str):
                    # Escape single quotes
                    escaped = val.replace("'", "''")
                    vals.append(f"'{escaped}'")
                elif isinstance(val, bool):
                    vals.append("TRUE" if val else "FALSE")
                elif isinstance(val, (int, float)):
                    vals.append(str(val))
                elif isinstance(val, dict) or isinstance(val, list):
                    import json
                    escaped = json.dumps(val).replace("'", "''")
                    vals.append(f"'{escaped}'")
                else:
                    vals.append(f"'{val}'")
            values_list.append(f"({', '.join(vals)})")

        sql = f"INSERT INTO {self._table} ({col_str}) VALUES {', '.join(values_list)}"

        # Execute with retry
        for attempt in range(self._config.max_retries):
            try:
                self._client.exec(sql)
                self._total_rows += len(rows)
                self._total_flushes += 1

                if self._config.on_flush:
                    self._config.on_flush(len(rows), self._total_rows)
                break

            except Exception as e:
                if attempt == self._config.max_retries - 1:
                    self._total_errors += 1
                    if self._config.on_error:
                        self._config.on_error(e)
                    raise
                time.sleep(0.1 * (attempt + 1))

    def _flush_loop(self) -> None:
        """Background flush loop."""
        while self._running:
            time.sleep(self._config.max_wait_seconds)
            with self._lock:
                if self._buffer:
                    try:
                        self._flush_internal()
                    except Exception:
                        pass  # Error callback already called

    def _make_dedup_key(self, row: Dict[str, Any]) -> tuple:
        """Create deduplication key from row."""
        if self._config.dedup_columns:
            return tuple(row.get(col) for col in self._config.dedup_columns)
        return tuple(sorted(row.items()))

    @property
    def pending_rows(self) -> int:
        """Number of rows pending flush."""
        with self._lock:
            return len(self._buffer)

    @property
    def stats(self) -> Dict[str, int]:
        """Get buffer statistics."""
        return {
            "total_rows": self._total_rows,
            "total_flushes": self._total_flushes,
            "total_errors": self._total_errors,
            "pending_rows": self.pending_rows,
        }
