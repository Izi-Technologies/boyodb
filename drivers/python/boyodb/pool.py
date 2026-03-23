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
from typing import Any, Callable, Dict, List, Optional, Union

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

    def _send_request_binary(self, conn: PooledConnection, request: Dict[str, Any], payload: bytes) -> None:
        """Send a binary request on a pooled connection."""
        sock = conn.socket

        if self._session_id:
            request["auth"] = self._session_id
        elif self._config.token:
            request["auth"] = self._config.token

        data = json.dumps(request).encode("utf-8")
        frame = struct.pack(">I", len(data)) + data
        payload_frame = struct.pack(">I", len(payload)) + payload

        sock.settimeout(self._config.write_timeout)
        try:
            sock.sendall(frame + payload_frame)
        except socket.timeout:
            conn.invalidate()
            raise TimeoutError("Write timeout")

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
            if response.get("status") != "ok":
                raise QueryError(response.get("message", "Ingest IPC failed"))
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

    def _send_binary(self, request: Dict[str, Any], payload: bytes) -> None:
        """Send a binary request."""
        if not self._conn:
            raise ConnectionError("No connection")
        self._pool._send_request_binary(self._conn, request, payload)


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

    # Health and Metadata Operations

    def health(self) -> None:
        """Check server health."""
        self._pool.health()

    def create_database(self, name: str) -> None:
        """Create a new database."""
        with self._pool.connection() as ctx:
            response = ctx._send({"op": "createdatabase", "name": name})
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Create database failed"))

    def create_table(self, database: str, table: str) -> None:
        """Create a new table."""
        with self._pool.connection() as ctx:
            response = ctx._send({
                "op": "createtable",
                "database": database,
                "table": table
            })
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Create table failed"))

    def list_databases(self) -> List[str]:
        """List all databases."""
        with self._pool.connection() as ctx:
            response = ctx._send({"op": "listdatabases"})
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "List databases failed"))
        return response.get("databases", [])

    def list_tables(self, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """List tables, optionally filtered by database."""
        request: Dict[str, Any] = {"op": "listtables"}
        if database:
            request["database"] = database
        with self._pool.connection() as ctx:
            response = ctx._send(request)
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "List tables failed"))
        return response.get("tables", [])

    def explain(self, sql: str) -> Dict[str, Any]:
        """Get query execution plan."""
        with self._pool.connection() as ctx:
            response = ctx._send({"op": "explain", "sql": sql})
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Explain failed"))
        return response.get("explain_plan", {})

    def metrics(self) -> Dict[str, Any]:
        """Get server metrics."""
        with self._pool.connection() as ctx:
            response = ctx._send({"op": "metrics"})
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Metrics failed"))
        return response.get("metrics", {})

    # Data Ingestion

    def ingest_csv(
        self,
        database: str,
        table: str,
        csv_data: bytes,
        has_header: bool = True,
        delimiter: Optional[str] = None,
    ) -> None:
        """Ingest CSV data into a table."""
        import base64
        request: Dict[str, Any] = {
            "op": "ingestcsv",
            "database": database,
            "table": table,
            "payload_base64": base64.b64encode(csv_data).decode("ascii"),
            "has_header": has_header,
        }
        if delimiter:
            request["delimiter"] = delimiter
        with self._pool.connection() as ctx:
            response = ctx._send(request)
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Ingest CSV failed"))

    def ingest_ipc(self, database: str, table: str, ipc_data: bytes) -> None:
        """Ingest Arrow IPC data into a table."""
        try:
            with self._pool.connection() as ctx:
                ctx._send_binary({
                    "op": "ingest_ipc_binary",
                    "database": database,
                    "table": table,
                }, ipc_data)
            return
        except Exception:
            pass
        
        import base64
        with self._pool.connection() as ctx:
            response = ctx._send({
                "op": "ingestipc",
                "database": database,
                "table": table,
                "payload_base64": base64.b64encode(ipc_data).decode("ascii"),
            })
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Ingest IPC failed"))

    # Prepared Statements

    def prepare(self, sql: str, database: Optional[str] = None) -> str:
        """Prepare a SELECT query on the server and return a prepared id."""
        db = database or self._config.database
        request: Dict[str, Any] = {"op": "prepare", "sql": sql}
        if db:
            request["database"] = db
        with self._pool.connection() as ctx:
            response = ctx._send(request)
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Prepare failed"))
        prepared_id = response.get("prepared_id")
        if not prepared_id:
            raise QueryError("missing prepared_id in response")
        return prepared_id

    def execute_prepared_binary(
        self,
        prepared_id: str,
        timeout: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Execute a prepared statement using binary IPC responses."""
        timeout_ms = timeout or self._config.query_timeout
        request: Dict[str, Any] = {
            "op": "execute_prepared_binary",
            "id": prepared_id,
            "timeout_millis": timeout_ms,
            "stream": True,
        }
        with self._pool.connection() as ctx:
            response = ctx._send(request)
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Execute prepared failed"))

        ipc_bytes = response.get("ipc_bytes")
        if ipc_bytes:
            return self._parse_arrow_ipc(ipc_bytes)

        ipc_base64 = response.get("ipc_base64")
        if ipc_base64:
            import base64
            ipc_data = base64.b64decode(ipc_base64)
            return self._parse_arrow_ipc(ipc_data)

        return []

    # Transaction Support

    def begin(self, isolation_level: Optional[str] = None, read_only: bool = False) -> None:
        """
        Start a new transaction.

        Args:
            isolation_level: Optional isolation level. One of:
                - "READ UNCOMMITTED"
                - "READ COMMITTED"
                - "REPEATABLE READ"
                - "SERIALIZABLE"
            read_only: If True, start a read-only transaction
        """
        if isolation_level:
            sql = f"START TRANSACTION ISOLATION LEVEL {isolation_level}"
            if read_only:
                sql += " READ ONLY"
        elif read_only:
            sql = "START TRANSACTION READ ONLY"
        else:
            sql = "BEGIN"
        self.exec(sql)

    def commit(self) -> None:
        """Commit the current transaction."""
        self.exec("COMMIT")

    def rollback(self, savepoint: Optional[str] = None) -> None:
        """
        Rollback the current transaction or to a savepoint.

        Args:
            savepoint: If provided, rollback to this savepoint
        """
        if savepoint:
            self.exec(f"ROLLBACK TO SAVEPOINT {savepoint}")
        else:
            self.exec("ROLLBACK")

    def savepoint(self, name: str) -> None:
        """Create a savepoint with the given name."""
        self.exec(f"SAVEPOINT {name}")

    def release_savepoint(self, name: str) -> None:
        """Release a savepoint."""
        self.exec(f"RELEASE SAVEPOINT {name}")

    def transaction(self, isolation_level: Optional[str] = None, read_only: bool = False):
        """
        Context manager for transactions.

        Usage:
            with client.transaction():
                client.exec("INSERT INTO ...")
                client.exec("UPDATE ...")
            # Automatically committed

        Args:
            isolation_level: Optional isolation level
            read_only: If True, start a read-only transaction

        Returns:
            Transaction context manager
        """
        return PooledTransactionContext(self, isolation_level, read_only)

    def in_transaction(self, fn) -> Any:
        """
        Execute a function within a transaction.

        If the function returns successfully, the transaction is committed.
        If an exception is raised, the transaction is rolled back.

        Args:
            fn: Function to execute (takes no arguments)

        Returns:
            Return value of the function
        """
        self.begin()
        try:
            result = fn()
            self.commit()
            return result
        except Exception:
            self.rollback()
            raise

    # Configuration

    def set_database(self, database: str) -> None:
        """Set the default database."""
        self._config.database = database

    # Vector Search Support

    def vector_search(
        self,
        query_vector: List[float],
        table: str,
        *,
        vector_column: str = "embedding",
        id_column: str = "id",
        metric: str = "cosine",
        limit: int = 10,
        filter_clause: Optional[str] = None,
        select_columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Perform vector similarity search.

        Args:
            query_vector: Query embedding vector
            table: Table to search
            vector_column: Column containing embeddings (default: "embedding")
            id_column: Column containing row IDs (default: "id")
            metric: Distance metric: "cosine", "euclidean", "dot", "manhattan"
            limit: Maximum number of results
            filter_clause: Optional SQL WHERE clause for filtering
            select_columns: Additional columns to return

        Returns:
            List of result dictionaries with id, score, and requested columns
        """
        vector_str = self._format_vector(query_vector)

        cols = [id_column]
        if select_columns:
            cols.extend(select_columns)
        select_list = ", ".join(cols)

        if metric == "cosine":
            sql = f"SELECT {select_list}, similarity({vector_column}, {vector_str}) AS score FROM {table}"
            order = "DESC"
        else:
            sql = f"SELECT {select_list}, vector_distance({vector_column}, {vector_str}, '{metric}') AS score FROM {table}"
            order = "ASC"

        if filter_clause:
            sql += f" WHERE {filter_clause}"

        sql += f" ORDER BY score {order} LIMIT {limit}"

        return self.query(sql)

    def hybrid_search(
        self,
        query_vector: List[float],
        text_query: str,
        table: str,
        *,
        vector_column: str = "embedding",
        text_column: str = "content",
        id_column: str = "id",
        vector_weight: float = 0.5,
        text_weight: float = 0.5,
        limit: int = 10,
        filter_clause: Optional[str] = None,
        select_columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Perform hybrid search combining vector similarity and text search.

        Args:
            query_vector: Query embedding vector
            text_query: Text query for BM25 search
            table: Table to search
            vector_column: Column containing embeddings
            text_column: Column containing text for full-text search
            id_column: Column containing row IDs
            vector_weight: Weight for vector similarity (0.0-1.0)
            text_weight: Weight for text relevance (0.0-1.0)
            limit: Maximum number of results
            filter_clause: Optional SQL WHERE clause
            select_columns: Additional columns to return

        Returns:
            List of result dictionaries with combined scores
        """
        vector_str = self._format_vector(query_vector)
        escaped_text = text_query.replace("'", "''")

        cols = [id_column]
        if select_columns:
            cols.extend(select_columns)
        select_list = ", ".join(cols)

        sql = f"""
            SELECT {select_list},
                   similarity({vector_column}, {vector_str}) * {vector_weight} AS vector_score,
                   COALESCE(match_score({text_column}, '{escaped_text}'), 0) * {text_weight} AS text_score,
                   similarity({vector_column}, {vector_str}) * {vector_weight} +
                   COALESCE(match_score({text_column}, '{escaped_text}'), 0) * {text_weight} AS combined_score
            FROM {table}
        """

        if filter_clause:
            sql += f" WHERE {filter_clause}"

        sql += f" ORDER BY combined_score DESC LIMIT {limit}"

        return self.query(sql)

    def _format_vector(self, vector: List[float]) -> str:
        """Format a vector as a SQL array literal."""
        values = ",".join(str(v) for v in vector)
        return f"ARRAY[{values}]"


class PooledTransactionContext:
    """Context manager for pooled client transactions."""

    def __init__(self, client: PooledClient, isolation_level: Optional[str], read_only: bool):
        self._client = client
        self._isolation_level = isolation_level
        self._read_only = read_only
        self._rolled_back = False

    def __enter__(self) -> "PooledTransactionContext":
        self._client.begin(self._isolation_level, self._read_only)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is not None:
            try:
                self._client.rollback()
            except Exception:
                pass
            return False

        if not self._rolled_back:
            self._client.commit()
        return False

    def rollback(self) -> None:
        """Explicitly rollback the transaction."""
        self._client.rollback()
        self._rolled_back = True

    def savepoint(self, name: str) -> None:
        """Create a savepoint."""
        self._client.savepoint(name)

    def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to a savepoint."""
        self._client.rollback(name)

    def release_savepoint(self, name: str) -> None:
        """Release a savepoint."""
        self._client.release_savepoint(name)
