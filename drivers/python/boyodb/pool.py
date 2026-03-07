"""
Connection pooling for boyodb Python client.
"""

import queue
import socket
import ssl
import struct
import json
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .errors import ConnectionError, QueryError, TimeoutError


@dataclass
class PoolConfig:
    """Configuration for connection pool."""

    host: str = "localhost"
    port: int = 8765

    # Pool settings
    pool_size: int = 10
    pool_timeout: float = 30.0

    # Connection settings
    tls: bool = False
    ca_file: Optional[str] = None
    insecure_skip_verify: bool = False
    connect_timeout: float = 10.0
    read_timeout: float = 30.0
    write_timeout: float = 10.0

    # Auth
    token: Optional[str] = None

    # Retry settings
    max_retries: int = 3
    retry_delay: float = 1.0

    # Query defaults
    database: Optional[str] = None
    query_timeout: int = 30000


class PooledConnection:
    """A connection managed by the pool."""

    def __init__(self, socket: socket.socket):
        self._socket = socket
        self._valid = True
        self._created_at = time.time()

    @property
    def socket(self) -> socket.socket:
        return self._socket

    @property
    def valid(self) -> bool:
        return self._valid and self._socket.fileno() != -1

    def invalidate(self):
        self._valid = False

    def close(self):
        self._valid = False
        try:
            self._socket.close()
        except Exception:
            pass


class ConnectionPool:
    """
    Thread-safe connection pool for BoyoDB.

    Example:
        pool = ConnectionPool(PoolConfig(host="localhost", port=8765, pool_size=10))

        # Get connection from pool
        with pool.connection() as conn:
            result = conn.query("SELECT * FROM mydb.users")

        # Connection automatically returned to pool
    """

    def __init__(self, config: PoolConfig):
        self._config = config
        self._pool: queue.Queue[PooledConnection] = queue.Queue(maxsize=config.pool_size)
        self._lock = threading.Lock()
        self._closed = False
        self._session_id: Optional[str] = None

        # Initialize pool
        for _ in range(config.pool_size):
            conn = self._create_connection()
            self._pool.put(conn)

    def _create_connection(self) -> PooledConnection:
        """Create a new connection."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self._config.connect_timeout)

        try:
            sock.connect((self._config.host, self._config.port))

            if self._config.tls:
                context = ssl.create_default_context()

                if self._config.ca_file:
                    context.load_verify_locations(self._config.ca_file)

                if self._config.insecure_skip_verify:
                    import warnings
                    warnings.warn(
                        "TLS certificate verification is DISABLED. "
                        "This is insecure and vulnerable to MITM attacks.",
                        UserWarning
                    )
                    context.check_hostname = False
                    context.verify_mode = ssl.CERT_NONE

                sock = context.wrap_socket(sock, server_hostname=self._config.host)

            return PooledConnection(sock)
        except Exception as e:
            sock.close()
            raise ConnectionError(f"Failed to create connection: {e}")

    def _borrow(self) -> PooledConnection:
        """Borrow a connection from the pool."""
        if self._closed:
            raise ConnectionError("Pool is closed")

        try:
            conn = self._pool.get(timeout=self._config.pool_timeout)

            if not conn.valid:
                conn.close()
                conn = self._create_connection()

            return conn
        except queue.Empty:
            raise ConnectionError("Connection pool exhausted")

    def _return(self, conn: PooledConnection):
        """Return a connection to the pool."""
        if conn.valid and not self._closed:
            try:
                self._pool.put_nowait(conn)
            except queue.Full:
                conn.close()
        else:
            conn.close()
            # Replace with new connection if not closed
            if not self._closed:
                try:
                    new_conn = self._create_connection()
                    self._pool.put_nowait(new_conn)
                except Exception:
                    pass

    def connection(self) -> 'PooledConnectionContext':
        """Get a connection context manager."""
        return PooledConnectionContext(self)

    def _send_request(self, conn: PooledConnection, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send a request on a pooled connection."""
        sock = conn.socket

        # Add auth
        if self._session_id:
            request["auth"] = self._session_id
        elif self._config.token:
            request["auth"] = self._config.token

        # Serialize
        data = json.dumps(request).encode("utf-8")
        frame = struct.pack(">I", len(data)) + data

        # Send
        sock.settimeout(self._config.write_timeout)
        try:
            sock.sendall(frame)
        except socket.timeout:
            conn.invalidate()
            raise TimeoutError("Write timeout")

        # Receive
        sock.settimeout(self._config.read_timeout)
        try:
            length_data = self._recv_exact(sock, 4)
            if not length_data:
                conn.invalidate()
                raise ConnectionError("Connection closed")

            length = struct.unpack(">I", length_data)[0]
            if length > 100 * 1024 * 1024:
                raise QueryError(f"Response too large: {length} bytes")

            response_data = self._recv_exact(sock, length)
            if not response_data:
                conn.invalidate()
                raise ConnectionError("Connection closed")

            response = json.loads(response_data.decode("utf-8"))

            # Handle streaming IPC
            if response.get("ipc_streaming"):
                chunks = []
                while True:
                    chunk_len_data = self._recv_exact(sock, 4)
                    if not chunk_len_data:
                        conn.invalidate()
                        raise ConnectionError("Connection closed")
                    chunk_len = struct.unpack(">I", chunk_len_data)[0]
                    if chunk_len == 0:
                        break
                    chunk = self._recv_exact(sock, chunk_len)
                    if not chunk:
                        conn.invalidate()
                        raise ConnectionError("Connection closed")
                    chunks.append(chunk)
                response["ipc_bytes"] = b"".join(chunks)

            # Handle fixed IPC
            elif response.get("ipc_len"):
                ipc_len = response["ipc_len"]
                payload_len_data = self._recv_exact(sock, 4)
                if not payload_len_data:
                    conn.invalidate()
                    raise ConnectionError("Connection closed")
                payload_len = struct.unpack(">I", payload_len_data)[0]
                if payload_len != ipc_len:
                    raise QueryError(f"IPC length mismatch: expected {ipc_len} got {payload_len}")
                payload = self._recv_exact(sock, payload_len)
                if not payload:
                    conn.invalidate()
                    raise ConnectionError("Connection closed")
                response["ipc_bytes"] = payload

            return response

        except socket.timeout:
            conn.invalidate()
            raise TimeoutError("Read timeout")

    def _recv_exact(self, sock: socket.socket, n: int) -> bytes:
        """Receive exactly n bytes."""
        data = b""
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return b""
            data += chunk
        return data

    def health(self) -> None:
        """Check server health."""
        with self.connection() as ctx:
            response = ctx._send({"op": "health"})
            if response.get("status") != "ok":
                raise QueryError(response.get("message", "Health check failed"))

    def login(self, username: str, password: str) -> None:
        """Login with username and password."""
        with self.connection() as ctx:
            response = ctx._send({
                "op": "login",
                "username": username,
                "password": password
            })
            if response.get("status") != "ok":
                raise QueryError(response.get("message", "Login failed"))
            self._session_id = response.get("session_id")

    def logout(self) -> None:
        """Logout from server."""
        with self.connection() as ctx:
            response = ctx._send({"op": "logout"})
            if response.get("status") != "ok":
                raise QueryError(response.get("message", "Logout failed"))
            self._session_id = None

    def close(self):
        """Close the pool and all connections."""
        self._closed = True
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break


class PooledConnectionContext:
    """Context manager for pooled connections."""

    def __init__(self, pool: ConnectionPool):
        self._pool = pool
        self._conn: Optional[PooledConnection] = None

    def __enter__(self) -> 'PooledConnectionContext':
        self._conn = self._pool._borrow()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            if exc_type is not None:
                self._conn.invalidate()
            self._pool._return(self._conn)
            self._conn = None
        return False

    def _send(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send a request."""
        if not self._conn:
            raise ConnectionError("No connection")
        return self._pool._send_request(self._conn, request)


class PooledClient:
    """
    High-performance pooled BoyoDB client.

    Example:
        from boyodb.pool import PooledClient, PoolConfig

        config = PoolConfig(
            host="localhost",
            port=8765,
            pool_size=20,
            database="analytics"
        )

        client = PooledClient(config)

        # Thread-safe concurrent queries
        result = client.query("SELECT COUNT(*) FROM events")
        print(result)

        client.close()
    """

    def __init__(self, config: PoolConfig):
        self._config = config
        self._pool = ConnectionPool(config)

        # Verify with health check
        self._pool.health()

    def query(self, sql: str, database: Optional[str] = None,
              timeout: Optional[int] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return rows."""
        db = database or self._config.database
        timeout_ms = timeout or self._config.query_timeout

        is_binary = sql.strip().lower().startswith(("select ", "with "))

        request: Dict[str, Any] = {
            "op": "query_binary" if is_binary else "query",
            "sql": sql,
            "timeout_millis": timeout_ms,
        }
        if is_binary:
            request["stream"] = True
        if db:
            request["database"] = db

        with self._pool.connection() as ctx:
            response = ctx._send(request)

        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Query failed"))

        # Parse IPC data
        ipc_bytes = response.get("ipc_bytes")
        if ipc_bytes:
            return self._parse_arrow_ipc(ipc_bytes)

        ipc_base64 = response.get("ipc_base64")
        if ipc_base64:
            import base64
            ipc_data = base64.b64decode(ipc_base64)
            return self._parse_arrow_ipc(ipc_data)

        return []

    def exec(self, sql: str, database: Optional[str] = None,
             timeout: Optional[int] = None) -> None:
        """Execute a SQL statement that doesn't return rows."""
        db = database or self._config.database
        timeout_ms = timeout or self._config.query_timeout

        request: Dict[str, Any] = {
            "op": "query",
            "sql": sql,
            "timeout_millis": timeout_ms,
        }
        if db:
            request["database"] = db

        with self._pool.connection() as ctx:
            response = ctx._send(request)

        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Exec failed"))

    def login(self, username: str, password: str) -> None:
        """Login with username and password."""
        self._pool.login(username, password)

    def logout(self) -> None:
        """Logout from server."""
        self._pool.logout()

    def close(self) -> None:
        """Close the client and release all connections."""
        self._pool.close()

    def __enter__(self) -> 'PooledClient':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _parse_arrow_ipc(self, data: bytes) -> List[Dict[str, Any]]:
        """Parse Arrow IPC data into rows."""
        # Try using pyarrow if available
        try:
            import pyarrow as pa
            reader = pa.ipc.open_stream(data)
            table = reader.read_all()
            return table.to_pylist()
        except ImportError:
            pass

        # Fall back to manual parsing (simplified)
        # This is a basic implementation - for production use pyarrow
        return self._parse_arrow_ipc_manual(data)

    def _parse_arrow_ipc_manual(self, data: bytes) -> List[Dict[str, Any]]:
        """Manual Arrow IPC parsing (simplified)."""
        # This is the same parser from client.py
        # For brevity, we import from there
        from .client import Client
        client = Client.__new__(Client)
        parsed = client._parse_arrow_ipc(data)
        return parsed.get("rows", [])
