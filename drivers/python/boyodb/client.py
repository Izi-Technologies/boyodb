"""
boyodb Python client implementation.
"""

import base64
import json
import socket
import ssl
import struct
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Union

from .errors import AuthError, ConnectionError, QueryError, TimeoutError


@dataclass
class Config:
    """Configuration options for the boyodb client."""

    # Enable TLS encryption
    tls: bool = False

    # Path to CA certificate file
    ca_file: Optional[str] = None

    # Skip TLS verification.
    # WARNING: SECURITY RISK - Disables certificate validation, making connections
    # vulnerable to man-in-the-middle (MITM) attacks. NEVER use in production.
    # Only use for local development or testing with self-signed certificates.
    insecure_skip_verify: bool = False

    # Connection timeout in seconds
    connect_timeout: float = 10.0

    # Read timeout in seconds
    read_timeout: float = 30.0

    # Write timeout in seconds
    write_timeout: float = 10.0

    # Authentication token
    token: Optional[str] = None

    # Maximum connection retry attempts
    max_retries: int = 3

    # Delay between retries in seconds
    retry_delay: float = 1.0

    # Default database for queries
    database: Optional[str] = None

    # Default query timeout in milliseconds
    query_timeout: int = 30000


@dataclass
class TableInfo:
    """Table metadata."""

    database: str
    name: str
    schema_json: Optional[str] = None


@dataclass
class QueryResult:
    """Result of a query."""

    rows: List[Dict[str, Any]] = field(default_factory=list)
    columns: List[str] = field(default_factory=list)
    segments_scanned: int = 0
    data_skipped_bytes: int = 0

    @property
    def row_count(self) -> int:
        """Number of rows in the result."""
        return len(self.rows)

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Iterate over rows."""
        return iter(self.rows)

    def __len__(self) -> int:
        """Number of rows."""
        return len(self.rows)

    def __getitem__(self, index: int) -> Dict[str, Any]:
        """Get row by index."""
        return self.rows[index]


class Client:
    """boyodb client for Python."""

    def __init__(self, host: str, config: Optional[Config] = None):
        """
        Create a new boyodb client.

        Args:
            host: Server address in host:port format
            config: Configuration options (uses defaults if None)
        """
        parts = host.split(":")
        self._hostname = parts[0] if parts else "localhost"
        self._port = int(parts[1]) if len(parts) > 1 else 8765

        self._config = config or Config()
        self._socket: Optional[socket.socket] = None
        self._session_id: Optional[str] = None

    def connect(self) -> None:
        """
        Connect to the server.

        Raises:
            ConnectionError: If connection fails
        """
        self._connect_with_retry()

        # Verify with health check
        try:
            self.health()
        except Exception as e:
            self.close()
            raise ConnectionError(f"Health check failed: {e}")

    def _connect_with_retry(self) -> None:
        """Connect with retry logic."""
        last_error = None

        for attempt in range(self._config.max_retries):
            try:
                self._connect_once()
                return
            except Exception as e:
                last_error = e
                if attempt < self._config.max_retries - 1:
                    time.sleep(self._config.retry_delay)

        raise ConnectionError(
            f"Failed to connect after {self._config.max_retries} attempts: {last_error}"
        )

    def _connect_once(self) -> None:
        """Single connection attempt."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self._config.connect_timeout)

        try:
            sock.connect((self._hostname, self._port))

            if self._config.tls:
                context = ssl.create_default_context()

                if self._config.ca_file:
                    context.load_verify_locations(self._config.ca_file)

                if self._config.insecure_skip_verify:
                    import warnings
                    warnings.warn(
                        "TLS certificate verification is DISABLED. "
                        "This is insecure and vulnerable to MITM attacks. "
                        "Only use for testing with self-signed certificates.",
                        UserWarning
                    )
                    context.check_hostname = False
                    context.verify_mode = ssl.CERT_NONE

                sock = context.wrap_socket(sock, server_hostname=self._hostname)

            self._socket = sock
        except Exception as e:
            sock.close()
            raise ConnectionError(f"Connection failed: {e}")

    def close(self) -> None:
        """Close the connection."""
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

    def __enter__(self) -> "Client":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def _send_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send a request to the server.

        Args:
            request: Request dictionary

        Returns:
            Response dictionary
        """
        if not self._socket:
            raise ConnectionError("Not connected")

        # Add auth if available
        if self._session_id:
            request["auth"] = self._session_id
        elif self._config.token:
            request["auth"] = self._config.token

        # Serialize request
        data = json.dumps(request).encode("utf-8")

        # Create length-prefixed frame
        frame = struct.pack(">I", len(data)) + data

        # Send with timeout
        self._socket.settimeout(self._config.write_timeout)
        try:
            self._socket.sendall(frame)
        except socket.timeout:
            raise TimeoutError("Write timeout")

        # Read response with timeout
        self._socket.settimeout(self._config.read_timeout)
        try:
            # Read length prefix
            length_data = self._recv_exact(4)
            if not length_data:
                raise ConnectionError("Connection closed")

            length = struct.unpack(">I", length_data)[0]

            # Sanity check
            if length > 100 * 1024 * 1024:
                raise QueryError(f"Response too large: {length} bytes")

            # Read response body
            response_data = self._recv_exact(length)
            if not response_data:
                raise ConnectionError("Connection closed")

            response = json.loads(response_data.decode("utf-8"))
            ipc_streaming = response.get("ipc_streaming")
            ipc_len = response.get("ipc_len")
            if ipc_streaming:
                payload = self._recv_stream_frames()
                response["ipc_bytes"] = payload
            elif ipc_len:
                if ipc_len > 100 * 1024 * 1024:
                    raise QueryError(f"Response too large: {ipc_len} bytes")
                payload_len_data = self._recv_exact(4)
                if not payload_len_data:
                    raise ConnectionError("Connection closed")
                payload_len = struct.unpack(">I", payload_len_data)[0]
                if payload_len != ipc_len:
                    raise QueryError(
                        f"IPC length mismatch: expected {ipc_len} got {payload_len}"
                    )
                payload = self._recv_exact(payload_len)
                if not payload:
                    raise ConnectionError("Connection closed")
                response["ipc_bytes"] = payload
            return response

        except socket.timeout:
            raise TimeoutError("Read timeout")

    def _recv_exact(self, n: int) -> bytes:
        """Receive exactly n bytes."""
        data = b""
        while len(data) < n:
            chunk = self._socket.recv(n - len(data))
            if not chunk:
                return b""
            data += chunk
        return data

    def _recv_stream_frames(self) -> bytes:
        chunks = []
        while True:
            length_data = self._recv_exact(4)
            if not length_data:
                raise ConnectionError("Connection closed")
            length = struct.unpack(">I", length_data)[0]
            if length == 0:
                break
            if length > 100 * 1024 * 1024:
                raise QueryError(f"Response too large: {length} bytes")
            chunk = self._recv_exact(length)
            if not chunk:
                raise ConnectionError("Connection closed")
            chunks.append(chunk)
        return b"".join(chunks)

    def health(self) -> None:
        """
        Check server health.

        Raises:
            QueryError: If health check fails
        """
        response = self._send_request({"op": "health"})
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Health check failed"))

    def login(self, username: str, password: str) -> None:
        """
        Login with username and password.

        Args:
            username: Username
            password: Password

        Raises:
            AuthError: If login fails
        """
        response = self._send_request(
            {"op": "login", "username": username, "password": password}
        )

        if response.get("status") != "ok":
            raise AuthError(response.get("message", "Login failed"))

        self._session_id = response.get("session_id")

    def logout(self) -> None:
        """
        Logout from the server.

        Raises:
            AuthError: If logout fails
        """
        response = self._send_request({"op": "logout"})

        if response.get("status") != "ok":
            raise AuthError(response.get("message", "Logout failed"))

        self._session_id = None

    def query(
        self,
        sql: str,
        database: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> QueryResult:
        """
        Execute a SQL query.

        Args:
            sql: SQL query string
            database: Database to use (overrides default)
            timeout: Query timeout in milliseconds (overrides default)

        Returns:
            QueryResult with rows and metadata
        """
        db = database or self._config.database
        timeout_ms = timeout or self._config.query_timeout

        request: Dict[str, Any] = {
            "op": "query_binary" if self._is_select_like(sql) else "query",
            "sql": sql,
            "timeout_millis": timeout_ms,
        }
        if request["op"] == "query_binary":
            request["stream"] = True

        if db:
            request["database"] = db

        response = self._send_request(request)

        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Query failed"))

        result = QueryResult(
            segments_scanned=response.get("segments_scanned", 0),
            data_skipped_bytes=response.get("data_skipped_bytes", 0),
        )

        # Parse Arrow IPC data if present
        ipc_bytes = response.get("ipc_bytes")
        ipc_base64 = response.get("ipc_base64")
        if ipc_bytes:
            parsed = self._parse_arrow_ipc(ipc_bytes)
            result.rows = parsed["rows"]
            result.columns = parsed["columns"]
        elif ipc_base64:
            ipc_data = base64.b64decode(ipc_base64)
            parsed = self._parse_arrow_ipc(ipc_data)
            result.rows = parsed["rows"]
            result.columns = parsed["columns"]

        return result

    def prepare(self, sql: str, database: Optional[str] = None) -> str:
        """
        Prepare a SELECT query on the server and return a prepared id.
        """
        db = database or self._config.database
        request: Dict[str, Any] = {"op": "prepare", "sql": sql}
        if db:
            request["database"] = db
        response = self._send_request(request)
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
    ) -> QueryResult:
        """
        Execute a prepared statement using binary IPC responses.
        """
        timeout_ms = timeout or self._config.query_timeout
        request: Dict[str, Any] = {
            "op": "execute_prepared_binary",
            "id": prepared_id,
            "timeout_millis": timeout_ms,
        }
        request["stream"] = True
        response = self._send_request(request)
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Execute prepared failed"))

        result = QueryResult(
            segments_scanned=response.get("segments_scanned", 0),
            data_skipped_bytes=response.get("data_skipped_bytes", 0),
        )
        ipc_bytes = response.get("ipc_bytes")
        ipc_base64 = response.get("ipc_base64")
        if ipc_bytes:
            parsed = self._parse_arrow_ipc(ipc_bytes)
            result.rows = parsed["rows"]
            result.columns = parsed["columns"]
        elif ipc_base64:
            ipc_data = base64.b64decode(ipc_base64)
            parsed = self._parse_arrow_ipc(ipc_data)
            result.rows = parsed["rows"]
            result.columns = parsed["columns"]

        return result

    def exec(
        self,
        sql: str,
        database: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> None:
        """
        Execute a SQL statement that doesn't return rows.

        Args:
            sql: SQL statement
            database: Database to use
            timeout: Timeout in milliseconds
        """
        self.query(sql, database, timeout)

    @staticmethod
    def _is_select_like(sql: str) -> bool:
        trimmed = sql.lstrip().lower()
        return trimmed.startswith("select ") or trimmed.startswith("with ")

    def _send_request_binary(self, request: Dict[str, Any], payload: bytes) -> None:
        if not self._socket:
            raise ConnectionError("Not connected")

        if self._session_id:
            request["auth"] = self._session_id
        elif self._config.token:
            request["auth"] = self._config.token

        data = json.dumps(request).encode("utf-8")
        frame = struct.pack(">I", len(data)) + data
        payload_frame = struct.pack(">I", len(payload)) + payload

        self._socket.settimeout(self._config.write_timeout)
        try:
            self._socket.sendall(frame + payload_frame)
        except socket.timeout:
            raise TimeoutError("Write timeout")

        self._socket.settimeout(self._config.read_timeout)
        length_data = self._recv_exact(4)
        if not length_data:
            raise ConnectionError("Connection closed")
        length = struct.unpack(">I", length_data)[0]
        if length > 100 * 1024 * 1024:
            raise QueryError(f"Response too large: {length} bytes")
        response_data = self._recv_exact(length)
        if not response_data:
            raise ConnectionError("Connection closed")
        response = json.loads(response_data.decode("utf-8"))
        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Ingest IPC failed"))

    def create_database(self, name: str) -> None:
        """
        Create a new database.

        Args:
            name: Database name
        """
        response = self._send_request({"op": "createdatabase", "name": name})

        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Create database failed"))

    def create_table(self, database: str, table: str) -> None:
        """
        Create a new table.

        Args:
            database: Database name
            table: Table name
        """
        response = self._send_request(
            {"op": "createtable", "database": database, "table": table}
        )

        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Create table failed"))

    def list_databases(self) -> List[str]:
        """
        List all databases.

        Returns:
            List of database names
        """
        response = self._send_request({"op": "listdatabases"})

        if response.get("status") != "ok":
            raise QueryError(response.get("message", "List databases failed"))

        return response.get("databases", [])

    def list_tables(self, database: Optional[str] = None) -> List[TableInfo]:
        """
        List tables, optionally filtered by database.

        Args:
            database: Database to filter by

        Returns:
            List of TableInfo objects
        """
        request: Dict[str, Any] = {"op": "listtables"}
        if database:
            request["database"] = database

        response = self._send_request(request)

        if response.get("status") != "ok":
            raise QueryError(response.get("message", "List tables failed"))

        return [
            TableInfo(
                database=t.get("database", ""),
                name=t.get("name", ""),
                schema_json=t.get("schema_json"),
            )
            for t in response.get("tables", [])
        ]

    def explain(self, sql: str) -> Dict[str, Any]:
        """
        Get query execution plan.

        Args:
            sql: SQL query

        Returns:
            Execution plan as dictionary
        """
        response = self._send_request({"op": "explain", "sql": sql})

        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Explain failed"))

        return response.get("explain_plan", {})

    def metrics(self) -> Dict[str, Any]:
        """
        Get server metrics.

        Returns:
            Metrics as dictionary
        """
        response = self._send_request({"op": "metrics"})

        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Metrics failed"))

        return response.get("metrics", {})

    def ingest_csv(
        self,
        database: str,
        table: str,
        csv_data: Union[str, bytes],
        has_header: bool = True,
        delimiter: Optional[str] = None,
    ) -> None:
        """
        Ingest CSV data into a table.

        Args:
            database: Database name
            table: Table name
            csv_data: CSV data as string or bytes
            has_header: Whether CSV has header row
            delimiter: Field delimiter (default ',')
        """
        if isinstance(csv_data, str):
            csv_data = csv_data.encode("utf-8")

        request: Dict[str, Any] = {
            "op": "ingestcsv",
            "database": database,
            "table": table,
            "payload_base64": base64.b64encode(csv_data).decode("ascii"),
            "has_header": has_header,
        }

        if delimiter:
            request["delimiter"] = delimiter

        response = self._send_request(request)

        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Ingest CSV failed"))

    def ingest_ipc(self, database: str, table: str, ipc_data: bytes) -> None:
        """
        Ingest Arrow IPC data into a table.

        Args:
            database: Database name
            table: Table name
            ipc_data: Arrow IPC data
        """
        try:
            self._send_request_binary(
                {
                    "op": "ingest_ipc_binary",
                    "database": database,
                    "table": table,
                },
                ipc_data,
            )
            return
        except Exception:
            pass

        response = self._send_request(
            {
                "op": "ingestipc",
                "database": database,
                "table": table,
                "payload_base64": base64.b64encode(ipc_data).decode("ascii"),
            }
        )

        if response.get("status") != "ok":
            raise QueryError(response.get("message", "Ingest IPC failed"))

    def set_database(self, database: str) -> None:
        """Set the default database."""
        self._config.database = database

    def set_token(self, token: str) -> None:
        """Set the authentication token."""
        self._config.token = token

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
            savepoint: If provided, rollback to this savepoint instead of
                      rolling back the entire transaction
        """
        if savepoint:
            self.exec(f"ROLLBACK TO SAVEPOINT {savepoint}")
        else:
            self.exec("ROLLBACK")

    def savepoint(self, name: str) -> None:
        """
        Create a savepoint with the given name.

        Args:
            name: Savepoint name
        """
        self.exec(f"SAVEPOINT {name}")

    def release_savepoint(self, name: str) -> None:
        """
        Release a savepoint.

        Args:
            name: Savepoint name to release
        """
        self.exec(f"RELEASE SAVEPOINT {name}")

    # =========================================================================
    # Pub/Sub (LISTEN/NOTIFY) Support
    # =========================================================================

    def listen(self, channel: str) -> None:
        """
        Subscribe to a notification channel.

        Args:
            channel: Channel name to listen to (use '*' for all channels)

        Example:
            client.listen("order_updates")
        """
        self.exec(f"LISTEN {channel}")

    def unlisten(self, channel: str = "*") -> None:
        """
        Unsubscribe from a notification channel.

        Args:
            channel: Channel name to unsubscribe from (default '*' for all)

        Example:
            client.unlisten("order_updates")
        """
        self.exec(f"UNLISTEN {channel}")

    def notify(self, channel: str, payload: Optional[str] = None) -> None:
        """
        Send a notification to a channel.

        Args:
            channel: Channel name to notify
            payload: Optional payload message (max 8000 bytes)

        Example:
            client.notify("order_updates", "order_id=12345")
        """
        if payload:
            escaped = payload.replace("'", "''")
            self.exec(f"NOTIFY {channel}, '{escaped}'")
        else:
            self.exec(f"NOTIFY {channel}")

    # =========================================================================
    # Trigger Management
    # =========================================================================

    def create_trigger(
        self,
        name: str,
        table: str,
        *,
        timing: str = "AFTER",
        events: List[str] = None,
        for_each_row: bool = True,
        when_condition: Optional[str] = None,
        function_name: Optional[str] = None,
        function_body: Optional[str] = None,
        or_replace: bool = False,
    ) -> None:
        """
        Create a trigger on a table.

        Args:
            name: Trigger name
            table: Table name (database.table format)
            timing: BEFORE, AFTER, or INSTEAD OF
            events: List of events: INSERT, UPDATE, DELETE, TRUNCATE
            for_each_row: True for row-level, False for statement-level
            when_condition: Optional WHEN clause
            function_name: Name of trigger function to call
            function_body: Inline function body (alternative to function_name)
            or_replace: If True, replace existing trigger

        Example:
            client.create_trigger(
                "audit_changes",
                "mydb.users",
                timing="AFTER",
                events=["INSERT", "UPDATE", "DELETE"],
                function_name="audit_log_func"
            )
        """
        if events is None:
            events = ["INSERT"]

        prefix = "CREATE OR REPLACE TRIGGER" if or_replace else "CREATE TRIGGER"
        event_str = " OR ".join(events)
        level = "FOR EACH ROW" if for_each_row else "FOR EACH STATEMENT"

        sql = f"{prefix} {name} {timing} {event_str} ON {table} {level}"

        if when_condition:
            sql += f" WHEN ({when_condition})"

        if function_name:
            sql += f" EXECUTE FUNCTION {function_name}()"
        elif function_body:
            sql += f" EXECUTE $$ {function_body} $$"

        self.exec(sql)

    def drop_trigger(self, name: str, table: str, if_exists: bool = False) -> None:
        """
        Drop a trigger from a table.

        Args:
            name: Trigger name
            table: Table name (database.table format)
            if_exists: If True, don't error if trigger doesn't exist
        """
        if_exists_clause = "IF EXISTS " if if_exists else ""
        self.exec(f"DROP TRIGGER {if_exists_clause}{name} ON {table}")

    def alter_trigger(self, name: str, table: str, enable: bool = True) -> None:
        """
        Enable or disable a trigger.

        Args:
            name: Trigger name
            table: Table name (database.table format)
            enable: True to enable, False to disable
        """
        action = "ENABLE" if enable else "DISABLE"
        self.exec(f"ALTER TRIGGER {name} {action} ON {table}")

    def list_triggers(
        self,
        database: Optional[str] = None,
        table: Optional[str] = None,
    ) -> QueryResult:
        """
        List triggers.

        Args:
            database: Optional database filter
            table: Optional table filter

        Returns:
            QueryResult with trigger information
        """
        if database and table:
            return self.query(f"SHOW TRIGGERS FROM {database}.{table}")
        elif database:
            return self.query(f"SHOW TRIGGERS FROM {database}")
        else:
            return self.query("SHOW TRIGGERS")

    # =========================================================================
    # Stored Procedures and Functions
    # =========================================================================

    def call(self, procedure: str, *args) -> QueryResult:
        """
        Call a stored procedure.

        Args:
            procedure: Procedure name
            *args: Arguments to pass to the procedure

        Returns:
            QueryResult with procedure output

        Example:
            result = client.call("process_order", 12345, "pending")
        """
        formatted_args = ", ".join(self._format_value(arg) for arg in args)
        return self.query(f"CALL {procedure}({formatted_args})")

    def create_function(
        self,
        name: str,
        parameters: List[tuple],
        return_type: str,
        body: str,
        *,
        language: str = "SQL",
        or_replace: bool = False,
        deterministic: bool = False,
    ) -> None:
        """
        Create a user-defined function.

        Args:
            name: Function name
            parameters: List of (name, type) tuples
            return_type: Return type
            body: Function body
            language: Language (SQL, PLSQL)
            or_replace: If True, replace existing function
            deterministic: If True, function is deterministic

        Example:
            client.create_function(
                "calculate_tax",
                [("amount", "DECIMAL"), ("rate", "DECIMAL")],
                "DECIMAL",
                "RETURN amount * rate",
                deterministic=True
            )
        """
        prefix = "CREATE OR REPLACE FUNCTION" if or_replace else "CREATE FUNCTION"
        params = ", ".join(f"{p[0]} {p[1]}" for p in parameters)
        det = "DETERMINISTIC " if deterministic else ""

        sql = f"{prefix} {name}({params}) RETURNS {return_type} {det}LANGUAGE {language} AS $$ {body} $$"
        self.exec(sql)

    def create_procedure(
        self,
        name: str,
        parameters: List[tuple],
        body: str,
        *,
        language: str = "SQL",
        or_replace: bool = False,
    ) -> None:
        """
        Create a stored procedure.

        Args:
            name: Procedure name
            parameters: List of (name, type, mode) tuples, mode is IN/OUT/INOUT
            body: Procedure body
            language: Language (SQL, PLSQL)
            or_replace: If True, replace existing procedure

        Example:
            client.create_procedure(
                "update_inventory",
                [("product_id", "INT", "IN"), ("quantity", "INT", "IN")],
                "UPDATE inventory SET qty = qty - quantity WHERE id = product_id"
            )
        """
        prefix = "CREATE OR REPLACE PROCEDURE" if or_replace else "CREATE PROCEDURE"
        params = ", ".join(
            f"{p[2] if len(p) > 2 else 'IN'} {p[0]} {p[1]}" for p in parameters
        )

        sql = f"{prefix} {name}({params}) LANGUAGE {language} AS $$ {body} $$"
        self.exec(sql)

    def drop_function(self, name: str, if_exists: bool = False) -> None:
        """Drop a user-defined function."""
        clause = "IF EXISTS " if if_exists else ""
        self.exec(f"DROP FUNCTION {clause}{name}")

    def drop_procedure(self, name: str, if_exists: bool = False) -> None:
        """Drop a stored procedure."""
        clause = "IF EXISTS " if if_exists else ""
        self.exec(f"DROP PROCEDURE {clause}{name}")

    def list_functions(self, pattern: Optional[str] = None) -> QueryResult:
        """List user-defined functions."""
        if pattern:
            return self.query(f"SHOW FUNCTIONS LIKE '{pattern}'")
        return self.query("SHOW FUNCTIONS")

    def _format_value(self, value: Any) -> str:
        """Format a Python value for SQL."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            return f"'{value.replace(chr(39), chr(39)+chr(39))}'"
        elif isinstance(value, (list, tuple)):
            return "ARRAY[" + ", ".join(self._format_value(v) for v in value) + "]"
        elif isinstance(value, dict):
            return f"'{json.dumps(value)}'"
        else:
            return f"'{str(value)}'"

    # =========================================================================
    # JSON Operations
    # =========================================================================

    def json_extract(self, json_column: str, path: str, table: str, where: Optional[str] = None) -> QueryResult:
        """
        Extract a value from a JSON column.

        Args:
            json_column: Name of JSON column
            path: JSON path (e.g., '$.name' or '$.address.city')
            table: Table name
            where: Optional WHERE clause

        Returns:
            QueryResult with extracted values

        Example:
            result = client.json_extract("data", "$.user.name", "events")
        """
        sql = f"SELECT {json_column} -> '{path}' AS value FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return self.query(sql)

    def json_extract_text(self, json_column: str, path: str, table: str, where: Optional[str] = None) -> QueryResult:
        """
        Extract a text value from a JSON column (unquoted).

        Args:
            json_column: Name of JSON column
            path: JSON path
            table: Table name
            where: Optional WHERE clause

        Returns:
            QueryResult with extracted text values
        """
        sql = f"SELECT {json_column} ->> '{path}' AS value FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return self.query(sql)

    def json_contains(self, json_column: str, json_value: dict, table: str) -> QueryResult:
        """
        Find rows where JSON column contains a value.

        Args:
            json_column: Name of JSON column
            json_value: JSON value to check for containment
            table: Table name

        Returns:
            QueryResult with matching rows

        Example:
            result = client.json_contains("metadata", {"status": "active"}, "users")
        """
        json_str = json.dumps(json_value).replace("'", "''")
        sql = f"SELECT * FROM {table} WHERE {json_column} @> '{json_str}'"
        return self.query(sql)

    # =========================================================================
    # AI/Vector Search Support
    # =========================================================================

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

        Example:
            results = client.vector_search(
                query_vector=embedding,
                table="documents",
                metric="cosine",
                limit=10,
                filter_clause="category = 'AI'",
                select_columns=["title", "content"],
            )
        """
        # Format vector as SQL array
        vector_str = self._format_vector(query_vector)

        # Build column list
        cols = [id_column]
        if select_columns:
            cols.extend(select_columns)
        select_list = ", ".join(cols)

        # Build query
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

    def transaction(self, isolation_level: Optional[str] = None, read_only: bool = False):
        """
        Context manager for transactions.

        Usage:
            with client.transaction():
                client.exec("INSERT INTO ...")
                client.exec("UPDATE ...")
            # Automatically committed

            with client.transaction() as tx:
                client.exec("INSERT INTO ...")
                if error:
                    tx.rollback()  # Explicit rollback
            # Automatically committed if not rolled back

        Args:
            isolation_level: Optional isolation level
            read_only: If True, start a read-only transaction

        Returns:
            Transaction context manager
        """
        return TransactionContext(self, isolation_level, read_only)


class TransactionContext:
    """Context manager for database transactions."""

    def __init__(self, client: Client, isolation_level: Optional[str], read_only: bool):
        self._client = client
        self._isolation_level = isolation_level
        self._read_only = read_only
        self._rolled_back = False

    def __enter__(self) -> "TransactionContext":
        self._client.begin(self._isolation_level, self._read_only)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is not None:
            # Exception occurred, rollback
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

    def _parse_arrow_ipc(self, data: bytes) -> Dict[str, Any]:
        """
        Parse Arrow IPC stream format.

        This is a simplified parser for common cases.
        For full Arrow support, use pyarrow.

        SECURITY NOTE: This parser performs basic bounds checking but is not a
        full implementation. For production use with untrusted data sources,
        consider using the official Apache Arrow library (pyarrow) which provides
        comprehensive validation and error handling. Install with: pip install boyodb[arrow]

        Args:
            data: IPC data bytes

        Returns:
            Dict with 'rows' and 'columns'
        """
        rows = []
        columns = []
        schema = None
        offset = 0

        while offset < len(data):
            # Read message length (4 bytes, little-endian)
            if offset + 4 > len(data):
                break

            msg_len = struct.unpack("<i", data[offset : offset + 4])[0]
            offset += 4

            if msg_len == 0:
                break

            # Handle continuation marker
            if msg_len == -1:
                if offset + 4 > len(data):
                    break
                msg_len = struct.unpack("<i", data[offset : offset + 4])[0]
                offset += 4
                if msg_len == 0:
                    break

            if offset + msg_len > len(data):
                break

            # Read metadata
            metadata = data[offset : offset + msg_len]
            offset += msg_len

            # Pad to 8-byte boundary
            padding = (8 - (msg_len % 8)) % 8
            offset += padding

            # Parse message type from flatbuffer
            if len(metadata) < 8:
                continue

            root_offset = struct.unpack("<I", metadata[0:4])[0]
            if root_offset >= len(metadata) - 4:
                continue

            vtable_offset = struct.unpack("<i", metadata[root_offset : root_offset + 4])[0]
            vtable_pos = root_offset - vtable_offset
            if vtable_pos < 0 or vtable_pos >= len(metadata) - 4:
                continue

            vtable_size = struct.unpack("<H", metadata[vtable_pos : vtable_pos + 2])[0]
            if vtable_size < 6 or vtable_pos + vtable_size > len(metadata):
                continue

            # Get header type
            if vtable_pos + 8 > len(metadata):
                continue
            header_type_offset = struct.unpack(
                "<H", metadata[vtable_pos + 6 : vtable_pos + 8]
            )[0]
            if header_type_offset == 0:
                continue

            header_type_pos = root_offset + header_type_offset
            if header_type_pos >= len(metadata):
                continue

            header_type = metadata[header_type_pos]

            # Get body length
            body_length = 0
            if vtable_pos + 10 <= len(metadata):
                body_length_offset = struct.unpack(
                    "<H", metadata[vtable_pos + 8 : vtable_pos + 10]
                )[0]
                if body_length_offset != 0:
                    body_length_pos = root_offset + body_length_offset
                    if body_length_pos + 8 <= len(metadata):
                        body_length = struct.unpack(
                            "<Q", metadata[body_length_pos : body_length_pos + 8]
                        )[0]

            if header_type == 1:
                # Schema message
                schema = self._parse_schema_flatbuffer(metadata, root_offset)
                columns = [f["name"] for f in schema.get("fields", [])]

            elif header_type == 3 and body_length > 0:
                # RecordBatch message
                if offset + body_length > len(data):
                    break
                body_data = data[offset : offset + body_length]
                offset += body_length

                if schema:
                    batch = self._parse_record_batch(
                        metadata, root_offset, body_data, schema
                    )
                    rows.extend(batch.get("rows", []))

            elif body_length > 0:
                offset += body_length

        return {"rows": rows, "columns": columns}

    def _parse_schema_flatbuffer(
        self, data: bytes, root_offset: int
    ) -> Dict[str, Any]:
        """Parse schema from flatbuffer."""
        schema: Dict[str, Any] = {"fields": []}

        if root_offset + 12 > len(data):
            return schema

        vtable_offset = struct.unpack("<i", data[root_offset : root_offset + 4])[0]
        vtable_pos = root_offset - vtable_offset
        if vtable_pos < 0 or vtable_pos + 10 > len(data):
            return schema

        vtable_size = struct.unpack("<H", data[vtable_pos : vtable_pos + 2])[0]
        if vtable_size < 10:
            return schema

        # Read fields vector offset
        if vtable_pos + 8 > len(data):
            return schema
        fields_offset = struct.unpack("<H", data[vtable_pos + 6 : vtable_pos + 8])[0]
        if fields_offset == 0:
            return schema

        fields_vector_pos = root_offset + fields_offset
        if fields_vector_pos + 4 > len(data):
            return schema

        vector_offset = struct.unpack(
            "<I", data[fields_vector_pos : fields_vector_pos + 4]
        )[0]
        vector_pos = fields_vector_pos + vector_offset
        if vector_pos + 4 > len(data):
            return schema

        num_fields = struct.unpack("<I", data[vector_pos : vector_pos + 4])[0]
        vector_pos += 4

        for i in range(min(num_fields, 100)):
            if vector_pos + 4 > len(data):
                break
            field_offset = struct.unpack("<I", data[vector_pos : vector_pos + 4])[0]
            field_pos = vector_pos + field_offset
            vector_pos += 4

            field = self._parse_field_flatbuffer(data, field_pos)
            schema["fields"].append(field)

        return schema

    def _parse_field_flatbuffer(self, data: bytes, pos: int) -> Dict[str, Any]:
        """Parse field from flatbuffer."""
        field = {"name": f"col{pos}", "type": "unknown", "nullable": False}

        if pos + 4 > len(data):
            return field

        vtable_offset = struct.unpack("<i", data[pos : pos + 4])[0]
        vtable_pos = pos - vtable_offset
        if vtable_pos < 0 or vtable_pos + 10 > len(data):
            return field

        vtable_size = struct.unpack("<H", data[vtable_pos : vtable_pos + 2])[0]
        if vtable_size < 8:
            return field

        # Name field
        if vtable_pos + 6 <= len(data):
            name_offset = struct.unpack("<H", data[vtable_pos + 4 : vtable_pos + 6])[0]
            if name_offset != 0:
                name_pos = pos + name_offset
                if name_pos + 4 <= len(data):
                    str_offset = struct.unpack("<I", data[name_pos : name_pos + 4])[0]
                    str_pos = name_pos + str_offset
                    if str_pos + 4 <= len(data):
                        str_len = struct.unpack("<I", data[str_pos : str_pos + 4])[0]
                        str_pos += 4
                        if str_pos + str_len <= len(data) and str_len < 256:
                            field["name"] = data[str_pos : str_pos + str_len].decode(
                                "utf-8", errors="replace"
                            )

        # Nullable field
        if vtable_pos + 8 <= len(data):
            nullable_offset = struct.unpack(
                "<H", data[vtable_pos + 6 : vtable_pos + 8]
            )[0]
            if nullable_offset != 0:
                nullable_pos = pos + nullable_offset
                if nullable_pos < len(data):
                    field["nullable"] = data[nullable_pos] != 0

        # Type field
        if vtable_pos + 10 <= len(data):
            type_offset = struct.unpack("<H", data[vtable_pos + 8 : vtable_pos + 10])[0]
            if type_offset != 0:
                type_pos = pos + type_offset
                if type_pos + 1 < len(data):
                    field["type"] = self._arrow_type_to_name(data[type_pos])

        return field

    def _arrow_type_to_name(self, t: int) -> str:
        """Convert Arrow type enum to type name."""
        types = {
            0: "null",
            1: "int64",
            2: "float64",
            3: "binary",
            4: "string",
            5: "bool",
            6: "decimal",
            7: "date",
            8: "time",
            9: "timestamp",
        }
        return types.get(t, "unknown")

    def _parse_record_batch(
        self,
        metadata: bytes,
        root_offset: int,
        body_data: bytes,
        schema: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Parse record batch."""
        batch: Dict[str, Any] = {"rows": [], "num_rows": 0}

        if root_offset + 4 > len(metadata):
            return batch

        vtable_offset = struct.unpack("<i", metadata[root_offset : root_offset + 4])[0]
        vtable_pos = root_offset - vtable_offset
        if vtable_pos < 0 or vtable_pos + 12 > len(metadata):
            return batch

        # Get header offset
        header_offset = struct.unpack("<H", metadata[vtable_pos + 8 : vtable_pos + 10])[
            0
        ]
        if header_offset == 0:
            return batch

        header_pos = root_offset + header_offset
        if header_pos + 4 > len(metadata):
            return batch

        header_table_offset = struct.unpack("<I", metadata[header_pos : header_pos + 4])[
            0
        ]
        record_batch_pos = header_pos + header_table_offset

        if record_batch_pos + 4 > len(metadata):
            return batch

        rb_vtable_offset = struct.unpack(
            "<i", metadata[record_batch_pos : record_batch_pos + 4]
        )[0]
        rb_vtable_pos = record_batch_pos - rb_vtable_offset
        if rb_vtable_pos < 0 or rb_vtable_pos + 12 > len(metadata):
            return batch

        # Get number of rows
        if rb_vtable_pos + 6 <= len(metadata):
            length_offset = struct.unpack(
                "<H", metadata[rb_vtable_pos + 4 : rb_vtable_pos + 6]
            )[0]
            if length_offset != 0:
                length_pos = record_batch_pos + length_offset
                if length_pos + 8 <= len(metadata):
                    batch["num_rows"] = struct.unpack(
                        "<Q", metadata[length_pos : length_pos + 8]
                    )[0]

        # Get buffers
        buffers = []
        if rb_vtable_pos + 10 <= len(metadata):
            buffers_offset = struct.unpack(
                "<H", metadata[rb_vtable_pos + 8 : rb_vtable_pos + 10]
            )[0]
            if buffers_offset != 0:
                buffers_vec_pos = record_batch_pos + buffers_offset
                if buffers_vec_pos + 4 <= len(metadata):
                    vec_offset = struct.unpack(
                        "<I", metadata[buffers_vec_pos : buffers_vec_pos + 4]
                    )[0]
                    vec_pos = buffers_vec_pos + vec_offset
                    if vec_pos + 4 <= len(metadata):
                        num_buffers = struct.unpack(
                            "<I", metadata[vec_pos : vec_pos + 4]
                        )[0]
                        vec_pos += 4
                        for _ in range(min(num_buffers, 1000)):
                            if vec_pos + 16 > len(metadata):
                                break
                            buf_offset = struct.unpack(
                                "<Q", metadata[vec_pos : vec_pos + 8]
                            )[0]
                            buf_length = struct.unpack(
                                "<Q", metadata[vec_pos + 8 : vec_pos + 16]
                            )[0]
                            buffers.append({"offset": buf_offset, "length": buf_length})
                            vec_pos += 16

        # Extract column data
        columns = []
        buf_idx = 0
        for field in schema.get("fields", []):
            col = {
                "name": field["name"],
                "type": field["type"],
                "nullable": field["nullable"],
                "null_bitmap": None,
                "data": None,
                "offsets": None,
            }

            # Validity buffer
            if buf_idx < len(buffers) and field["nullable"]:
                buf = buffers[buf_idx]
                if (
                    buf["offset"] >= 0
                    and buf["offset"] + buf["length"] <= len(body_data)
                ):
                    col["null_bitmap"] = body_data[
                        buf["offset"] : buf["offset"] + buf["length"]
                    ]
                buf_idx += 1

            # Offsets for variable-length types
            if field["type"] in ("string", "binary"):
                if buf_idx < len(buffers):
                    buf = buffers[buf_idx]
                    if (
                        buf["offset"] >= 0
                        and buf["offset"] + buf["length"] <= len(body_data)
                    ):
                        offset_data = body_data[
                            buf["offset"] : buf["offset"] + buf["length"]
                        ]
                        col["offsets"] = []
                        for j in range(0, len(offset_data), 4):
                            if j + 4 <= len(offset_data):
                                col["offsets"].append(
                                    struct.unpack("<i", offset_data[j : j + 4])[0]
                                )
                    buf_idx += 1

            # Data buffer
            if buf_idx < len(buffers):
                buf = buffers[buf_idx]
                if (
                    buf["offset"] >= 0
                    and buf["offset"] + buf["length"] <= len(body_data)
                ):
                    col["data"] = body_data[
                        buf["offset"] : buf["offset"] + buf["length"]
                    ]
                buf_idx += 1

            columns.append(col)

        # Build row objects
        for row in range(batch["num_rows"]):
            row_obj = {}
            for col in columns:
                row_obj[col["name"]] = self._extract_value(col, row)
            batch["rows"].append(row_obj)

        return batch

    def _extract_value(self, col: Dict[str, Any], row: int) -> Any:
        """Extract a value from a column at a specific row."""
        # Check for null
        if col["nullable"] and col["null_bitmap"]:
            byte_idx = row // 8
            bit_idx = row % 8
            if byte_idx < len(col["null_bitmap"]):
                if (col["null_bitmap"][byte_idx] & (1 << bit_idx)) == 0:
                    return None

        data = col["data"]
        if not data:
            return None

        col_type = col["type"]

        if col_type == "bool":
            byte_idx = row // 8
            bit_idx = row % 8
            if byte_idx < len(data):
                return (data[byte_idx] & (1 << bit_idx)) != 0
            return None

        elif col_type in ("int64", "timestamp"):
            offset = row * 8
            if offset + 8 <= len(data):
                return struct.unpack("<q", data[offset : offset + 8])[0]
            return None

        elif col_type == "float64":
            offset = row * 8
            if offset + 8 <= len(data):
                return struct.unpack("<d", data[offset : offset + 8])[0]
            return None

        elif col_type == "string":
            offsets = col["offsets"]
            if not offsets or row + 1 >= len(offsets):
                return None
            start = offsets[row]
            end = offsets[row + 1]
            if start >= 0 and end <= len(data):
                return data[start:end].decode("utf-8", errors="replace")
            return None

        elif col_type == "binary":
            offsets = col["offsets"]
            if not offsets or row + 1 >= len(offsets):
                return None
            start = offsets[row]
            end = offsets[row + 1]
            if start >= 0 and end <= len(data):
                return data[start:end]
            return None

        return None


# =============================================================================
# AI/Vector Utilities
# =============================================================================

@dataclass
class EmbeddingModel:
    """Embedding model configuration."""
    id: str
    provider: str
    dimensions: int
    max_tokens: int


# Common embedding models
EMBEDDING_MODELS: Dict[str, EmbeddingModel] = {
    # OpenAI
    "text-embedding-3-small": EmbeddingModel("text-embedding-3-small", "openai", 1536, 8191),
    "text-embedding-3-large": EmbeddingModel("text-embedding-3-large", "openai", 3072, 8191),
    "text-embedding-ada-002": EmbeddingModel("text-embedding-ada-002", "openai", 1536, 8191),
    # Open source
    "all-MiniLM-L6-v2": EmbeddingModel("all-MiniLM-L6-v2", "huggingface", 384, 256),
    "all-mpnet-base-v2": EmbeddingModel("all-mpnet-base-v2", "huggingface", 768, 384),
    "bge-small-en-v1.5": EmbeddingModel("bge-small-en-v1.5", "huggingface", 384, 512),
    "bge-base-en-v1.5": EmbeddingModel("bge-base-en-v1.5", "huggingface", 768, 512),
    "bge-large-en-v1.5": EmbeddingModel("bge-large-en-v1.5", "huggingface", 1024, 512),
}


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two vectors."""
    if len(a) != len(b) or len(a) == 0:
        return 0.0

    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5

    if norm_a == 0 or norm_b == 0:
        return 0.0

    return dot / (norm_a * norm_b)


def euclidean_distance(a: List[float], b: List[float]) -> float:
    """Compute Euclidean (L2) distance between two vectors."""
    if len(a) != len(b):
        return 0.0

    return sum((x - y) ** 2 for x, y in zip(a, b)) ** 0.5


def dot_product(a: List[float], b: List[float]) -> float:
    """Compute dot product of two vectors."""
    if len(a) != len(b):
        return 0.0

    return sum(x * y for x, y in zip(a, b))


def normalize_vector(v: List[float]) -> List[float]:
    """Normalize a vector to unit length."""
    norm = sum(x * x for x in v) ** 0.5
    if norm == 0:
        return v
    return [x / norm for x in v]


def chunk_text(
    text: str,
    chunk_size: int = 512,
    overlap: int = 50,
) -> List[str]:
    """
    Split text into overlapping chunks suitable for embedding.

    Args:
        text: Text to split
        chunk_size: Target number of words per chunk
        overlap: Number of words to overlap between chunks

    Returns:
        List of text chunks
    """
    words = text.split()
    if len(words) <= chunk_size:
        return [text]

    chunks = []
    step = max(1, chunk_size - overlap)

    for i in range(0, len(words), step):
        chunk_words = words[i : i + chunk_size]
        chunks.append(" ".join(chunk_words))
        if i + chunk_size >= len(words):
            break

    return chunks


def chunk_by_sentences(
    text: str,
    max_tokens: int = 512,
    overlap_sentences: int = 1,
) -> List[str]:
    """
    Split text by sentences with overlap.

    Args:
        text: Text to split
        max_tokens: Maximum words per chunk
        overlap_sentences: Number of sentences to overlap

    Returns:
        List of text chunks
    """
    import re

    # Simple sentence splitting
    sentences = re.split(r"(?<=[.!?])\s+", text)
    sentences = [s.strip() for s in sentences if s.strip()]

    if not sentences:
        return [text] if text.strip() else []

    chunks = []
    current_chunk: List[str] = []
    current_len = 0

    for sentence in sentences:
        sentence_len = len(sentence.split())

        if current_len + sentence_len > max_tokens and current_chunk:
            chunks.append(" ".join(current_chunk))
            # Keep overlap
            keep = max(0, len(current_chunk) - overlap_sentences)
            current_chunk = current_chunk[keep:]
            current_len = sum(len(s.split()) for s in current_chunk)

        current_chunk.append(sentence)
        current_len += sentence_len

    if current_chunk:
        chunks.append(" ".join(current_chunk))

    return chunks


# =============================================================================
# Additional Client Methods - Added to Client class via monkey-patching style
# or as mixin. Here we extend the Client class directly.
# =============================================================================

# Extend Client class with additional methods

def _add_client_methods():
    """Add additional methods to the Client class."""

    # =========================================================================
    # Cursor Support
    # =========================================================================

    def declare_cursor(
        self,
        name: str,
        query: str,
        *,
        scroll: bool = False,
        hold: bool = False,
    ) -> None:
        """
        Declare a server-side cursor for large result sets.

        Args:
            name: Cursor name
            query: SELECT query for the cursor
            scroll: If True, cursor can move backwards (SCROLL)
            hold: If True, cursor persists after transaction (WITH HOLD)

        Example:
            client.begin()
            client.declare_cursor("my_cursor", "SELECT * FROM large_table")
            while True:
                rows = client.fetch_cursor("my_cursor", 100)
                if not rows:
                    break
                process(rows)
            client.close_cursor("my_cursor")
            client.commit()
        """
        options = []
        if scroll:
            options.append("SCROLL")
        if hold:
            options.append("WITH HOLD")
        opts_str = " ".join(options)
        if opts_str:
            self.exec(f"DECLARE {name} {opts_str} CURSOR FOR {query}")
        else:
            self.exec(f"DECLARE {name} CURSOR FOR {query}")

    def fetch_cursor(
        self,
        name: str,
        count: int = 1,
        direction: str = "NEXT",
    ) -> QueryResult:
        """
        Fetch rows from a cursor.

        Args:
            name: Cursor name
            count: Number of rows to fetch
            direction: NEXT, PRIOR, FIRST, LAST, ABSOLUTE n, RELATIVE n,
                      FORWARD, FORWARD n, FORWARD ALL, BACKWARD, BACKWARD n, BACKWARD ALL

        Returns:
            QueryResult with fetched rows
        """
        if direction.upper() in ("NEXT", "PRIOR", "FIRST", "LAST"):
            return self.query(f"FETCH {direction} FROM {name}")
        elif direction.upper().startswith(("ABSOLUTE", "RELATIVE")):
            return self.query(f"FETCH {direction} FROM {name}")
        else:
            return self.query(f"FETCH {direction} {count} FROM {name}")

    def move_cursor(self, name: str, count: int = 1, direction: str = "NEXT") -> None:
        """
        Move cursor position without returning rows.

        Args:
            name: Cursor name
            count: Number of positions to move
            direction: Movement direction (same as fetch_cursor)
        """
        if direction.upper() in ("NEXT", "PRIOR", "FIRST", "LAST"):
            self.exec(f"MOVE {direction} FROM {name}")
        else:
            self.exec(f"MOVE {direction} {count} FROM {name}")

    def close_cursor(self, name: str) -> None:
        """
        Close a cursor and free resources.

        Args:
            name: Cursor name
        """
        self.exec(f"CLOSE {name}")

    # =========================================================================
    # Large Objects (BLOB/CLOB) Support
    # =========================================================================

    def lo_create(self) -> int:
        """
        Create a new large object.

        Returns:
            OID of the created large object
        """
        result = self.query("SELECT lo_create(0) AS oid")
        return result.rows[0]["oid"]

    def lo_open(self, oid: int, mode: str = "rw") -> int:
        """
        Open a large object for reading/writing.

        Args:
            oid: Large object OID
            mode: 'r' for read, 'w' for write, 'rw' for both

        Returns:
            File descriptor for the large object
        """
        mode_flag = 0x20000 if "w" in mode else 0  # INV_WRITE
        mode_flag |= 0x40000 if "r" in mode else 0  # INV_READ
        result = self.query(f"SELECT lo_open({oid}, {mode_flag}) AS fd")
        return result.rows[0]["fd"]

    def lo_write(self, fd: int, data: bytes) -> int:
        """
        Write data to a large object.

        Args:
            fd: File descriptor from lo_open
            data: Data to write

        Returns:
            Number of bytes written
        """
        encoded = base64.b64encode(data).decode("ascii")
        result = self.query(f"SELECT lo_write({fd}, decode('{encoded}', 'base64')) AS written")
        return result.rows[0]["written"]

    def lo_read(self, fd: int, length: int) -> bytes:
        """
        Read data from a large object.

        Args:
            fd: File descriptor from lo_open
            length: Number of bytes to read

        Returns:
            Read bytes
        """
        result = self.query(f"SELECT encode(lo_read({fd}, {length}), 'base64') AS data")
        if result.rows and result.rows[0]["data"]:
            return base64.b64decode(result.rows[0]["data"])
        return b""

    def lo_seek(self, fd: int, offset: int, whence: int = 0) -> int:
        """
        Seek to a position in a large object.

        Args:
            fd: File descriptor
            offset: Byte offset
            whence: 0=SEEK_SET, 1=SEEK_CUR, 2=SEEK_END

        Returns:
            New position
        """
        result = self.query(f"SELECT lo_lseek({fd}, {offset}, {whence}) AS pos")
        return result.rows[0]["pos"]

    def lo_tell(self, fd: int) -> int:
        """Get current position in large object."""
        result = self.query(f"SELECT lo_tell({fd}) AS pos")
        return result.rows[0]["pos"]

    def lo_close(self, fd: int) -> None:
        """Close a large object file descriptor."""
        self.exec(f"SELECT lo_close({fd})")

    def lo_unlink(self, oid: int) -> None:
        """Delete a large object."""
        self.exec(f"SELECT lo_unlink({oid})")

    def lo_import(self, data: bytes) -> int:
        """
        Import data as a large object (convenience method).

        Args:
            data: Binary data to store

        Returns:
            OID of created large object
        """
        oid = self.lo_create()
        fd = self.lo_open(oid, "w")
        try:
            self.lo_write(fd, data)
        finally:
            self.lo_close(fd)
        return oid

    def lo_export(self, oid: int) -> bytes:
        """
        Export a large object as bytes (convenience method).

        Args:
            oid: Large object OID

        Returns:
            Complete binary data
        """
        fd = self.lo_open(oid, "r")
        try:
            chunks = []
            while True:
                chunk = self.lo_read(fd, 65536)
                if not chunk:
                    break
                chunks.append(chunk)
            return b"".join(chunks)
        finally:
            self.lo_close(fd)

    # =========================================================================
    # Advisory Locks
    # =========================================================================

    def advisory_lock(self, key: int, shared: bool = False) -> bool:
        """
        Acquire an advisory lock (blocks until acquired).

        Args:
            key: Lock key (64-bit integer)
            shared: If True, acquire shared lock; else exclusive

        Returns:
            True if lock acquired
        """
        func = "pg_advisory_lock_shared" if shared else "pg_advisory_lock"
        self.exec(f"SELECT {func}({key})")
        return True

    def advisory_lock_try(self, key: int, shared: bool = False) -> bool:
        """
        Try to acquire an advisory lock (non-blocking).

        Args:
            key: Lock key
            shared: If True, try shared lock

        Returns:
            True if lock acquired, False if not available
        """
        func = "pg_try_advisory_lock_shared" if shared else "pg_try_advisory_lock"
        result = self.query(f"SELECT {func}({key}) AS acquired")
        return result.rows[0]["acquired"]

    def advisory_unlock(self, key: int, shared: bool = False) -> bool:
        """
        Release an advisory lock.

        Args:
            key: Lock key
            shared: If True, release shared lock

        Returns:
            True if lock was released
        """
        func = "pg_advisory_unlock_shared" if shared else "pg_advisory_unlock"
        result = self.query(f"SELECT {func}({key}) AS released")
        return result.rows[0]["released"]

    def advisory_unlock_all(self) -> None:
        """Release all advisory locks held by this session."""
        self.exec("SELECT pg_advisory_unlock_all()")

    def advisory_xact_lock(self, key: int, shared: bool = False) -> bool:
        """
        Acquire a transaction-level advisory lock (auto-released on commit/rollback).

        Args:
            key: Lock key
            shared: If True, acquire shared lock
        """
        func = "pg_advisory_xact_lock_shared" if shared else "pg_advisory_xact_lock"
        self.exec(f"SELECT {func}({key})")
        return True

    # =========================================================================
    # Full-Text Search
    # =========================================================================

    def fts_search(
        self,
        table: str,
        text_column: str,
        query: str,
        *,
        config: str = "english",
        limit: int = 100,
        rank: bool = True,
        select_columns: Optional[List[str]] = None,
    ) -> QueryResult:
        """
        Perform full-text search on a table.

        Args:
            table: Table name
            text_column: Column containing text to search
            query: Search query (supports & | ! operators)
            config: Text search configuration (english, simple, etc.)
            limit: Maximum results
            rank: If True, include relevance ranking
            select_columns: Additional columns to return

        Returns:
            QueryResult with matching rows

        Example:
            results = client.fts_search(
                "articles",
                "content",
                "database & performance",
                limit=10
            )
        """
        escaped_query = query.replace("'", "''")
        cols = select_columns or ["*"]
        select_list = ", ".join(cols)

        if rank:
            sql = f"""
                SELECT {select_list},
                       ts_rank(to_tsvector('{config}', {text_column}),
                               to_tsquery('{config}', '{escaped_query}')) AS rank
                FROM {table}
                WHERE to_tsvector('{config}', {text_column}) @@ to_tsquery('{config}', '{escaped_query}')
                ORDER BY rank DESC
                LIMIT {limit}
            """
        else:
            sql = f"""
                SELECT {select_list}
                FROM {table}
                WHERE to_tsvector('{config}', {text_column}) @@ to_tsquery('{config}', '{escaped_query}')
                LIMIT {limit}
            """
        return self.query(sql)

    def fts_highlight(
        self,
        table: str,
        text_column: str,
        query: str,
        *,
        config: str = "english",
        start_tag: str = "<b>",
        stop_tag: str = "</b>",
        max_words: int = 35,
        limit: int = 10,
    ) -> QueryResult:
        """
        Search with highlighted snippets.

        Args:
            table: Table name
            text_column: Text column to search
            query: Search query
            config: Text search config
            start_tag: Tag to wrap matches
            stop_tag: Closing tag
            max_words: Max words in snippet
            limit: Max results

        Returns:
            QueryResult with 'headline' column containing highlighted text
        """
        escaped_query = query.replace("'", "''")
        sql = f"""
            SELECT *,
                   ts_headline('{config}', {text_column},
                               to_tsquery('{config}', '{escaped_query}'),
                               'StartSel={start_tag}, StopSel={stop_tag}, MaxWords={max_words}') AS headline
            FROM {table}
            WHERE to_tsvector('{config}', {text_column}) @@ to_tsquery('{config}', '{escaped_query}')
            LIMIT {limit}
        """
        return self.query(sql)

    # =========================================================================
    # Geospatial / GIS Support
    # =========================================================================

    def geo_distance(
        self,
        table: str,
        geo_column: str,
        lat: float,
        lon: float,
        radius_meters: float,
        *,
        select_columns: Optional[List[str]] = None,
        limit: int = 100,
    ) -> QueryResult:
        """
        Find points within a radius of a location.

        Args:
            table: Table name
            geo_column: Geometry/geography column
            lat: Latitude of center point
            lon: Longitude of center point
            radius_meters: Search radius in meters
            select_columns: Columns to return
            limit: Max results

        Returns:
            QueryResult with matching rows and distance
        """
        cols = select_columns or ["*"]
        select_list = ", ".join(cols)
        sql = f"""
            SELECT {select_list},
                   ST_Distance({geo_column}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)) AS distance
            FROM {table}
            WHERE ST_DWithin({geo_column}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326), {radius_meters})
            ORDER BY distance
            LIMIT {limit}
        """
        return self.query(sql)

    def geo_contains(
        self,
        table: str,
        geo_column: str,
        wkt: str,
        *,
        select_columns: Optional[List[str]] = None,
    ) -> QueryResult:
        """
        Find geometries contained within a polygon.

        Args:
            table: Table name
            geo_column: Geometry column
            wkt: Well-Known Text of containing polygon
            select_columns: Columns to return

        Returns:
            QueryResult with contained geometries
        """
        cols = select_columns or ["*"]
        select_list = ", ".join(cols)
        escaped_wkt = wkt.replace("'", "''")
        sql = f"""
            SELECT {select_list}
            FROM {table}
            WHERE ST_Contains(ST_GeomFromText('{escaped_wkt}', 4326), {geo_column})
        """
        return self.query(sql)

    def geo_nearest(
        self,
        table: str,
        geo_column: str,
        lat: float,
        lon: float,
        k: int = 10,
        *,
        select_columns: Optional[List[str]] = None,
    ) -> QueryResult:
        """
        Find k nearest neighbors to a point.

        Args:
            table: Table name
            geo_column: Geometry column
            lat: Latitude
            lon: Longitude
            k: Number of neighbors
            select_columns: Columns to return

        Returns:
            QueryResult with nearest rows
        """
        cols = select_columns or ["*"]
        select_list = ", ".join(cols)
        sql = f"""
            SELECT {select_list},
                   ST_Distance({geo_column}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)) AS distance
            FROM {table}
            ORDER BY {geo_column} <-> ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)
            LIMIT {k}
        """
        return self.query(sql)

    # =========================================================================
    # Time Travel Queries
    # =========================================================================

    def query_as_of(
        self,
        sql: str,
        timestamp: str,
        *,
        database: Optional[str] = None,
    ) -> QueryResult:
        """
        Execute a query as of a specific point in time (time travel).

        Args:
            sql: SQL query
            timestamp: ISO timestamp or interval (e.g., '2024-01-01 12:00:00' or '-1 hour')
            database: Database to use

        Returns:
            QueryResult with historical data

        Example:
            # Query data as it was 1 hour ago
            result = client.query_as_of(
                "SELECT * FROM orders",
                "-1 hour"
            )

            # Query data as of specific timestamp
            result = client.query_as_of(
                "SELECT * FROM inventory",
                "2024-01-15 09:00:00"
            )
        """
        escaped_ts = timestamp.replace("'", "''")
        # Wrap query with AS OF clause
        time_travel_sql = f"SELECT * FROM ({sql}) AS q FOR SYSTEM_TIME AS OF '{escaped_ts}'"
        return self.query(time_travel_sql, database=database)

    def query_between(
        self,
        sql: str,
        start_time: str,
        end_time: str,
        *,
        database: Optional[str] = None,
    ) -> QueryResult:
        """
        Query all versions of rows between two timestamps.

        Args:
            sql: SQL query
            start_time: Start timestamp
            end_time: End timestamp
            database: Database to use

        Returns:
            QueryResult with all row versions in the time range
        """
        escaped_start = start_time.replace("'", "''")
        escaped_end = end_time.replace("'", "''")
        time_travel_sql = f"SELECT * FROM ({sql}) AS q FOR SYSTEM_TIME BETWEEN '{escaped_start}' AND '{escaped_end}'"
        return self.query(time_travel_sql, database=database)

    # =========================================================================
    # Batch Operations
    # =========================================================================

    def batch_insert(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        *,
        chunk_size: int = 1000,
    ) -> int:
        """
        Insert multiple rows efficiently.

        Args:
            table: Table name
            rows: List of row dictionaries
            chunk_size: Rows per INSERT statement

        Returns:
            Number of rows inserted

        Example:
            client.batch_insert("users", [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
            ])
        """
        if not rows:
            return 0

        total = 0
        columns = list(rows[0].keys())
        col_list = ", ".join(columns)

        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            values = []
            for row in chunk:
                row_values = ", ".join(self._format_value(row.get(c)) for c in columns)
                values.append(f"({row_values})")

            values_str = ", ".join(values)
            self.exec(f"INSERT INTO {table} ({col_list}) VALUES {values_str}")
            total += len(chunk)

        return total

    def batch_update(
        self,
        table: str,
        updates: List[Dict[str, Any]],
        key_column: str,
    ) -> int:
        """
        Update multiple rows efficiently.

        Args:
            table: Table name
            updates: List of dicts with key column and columns to update
            key_column: Column to match rows on

        Returns:
            Number of rows updated
        """
        if not updates:
            return 0

        total = 0
        for row in updates:
            key_value = self._format_value(row[key_column])
            set_clauses = ", ".join(
                f"{col} = {self._format_value(val)}"
                for col, val in row.items()
                if col != key_column
            )
            self.exec(f"UPDATE {table} SET {set_clauses} WHERE {key_column} = {key_value}")
            total += 1

        return total

    def batch_upsert(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> int:
        """
        Insert or update rows (upsert).

        Args:
            table: Table name
            rows: Rows to upsert
            conflict_columns: Columns that define uniqueness
            update_columns: Columns to update on conflict (default: all non-conflict)

        Returns:
            Number of rows affected
        """
        if not rows:
            return 0

        columns = list(rows[0].keys())
        col_list = ", ".join(columns)
        conflict_list = ", ".join(conflict_columns)

        if update_columns is None:
            update_columns = [c for c in columns if c not in conflict_columns]

        update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)

        values = []
        for row in rows:
            row_values = ", ".join(self._format_value(row.get(c)) for c in columns)
            values.append(f"({row_values})")

        values_str = ", ".join(values)
        sql = f"""
            INSERT INTO {table} ({col_list}) VALUES {values_str}
            ON CONFLICT ({conflict_list}) DO UPDATE SET {update_clause}
        """
        self.exec(sql)
        return len(rows)

    def batch_delete(
        self,
        table: str,
        keys: List[Any],
        key_column: str,
    ) -> int:
        """
        Delete multiple rows by key.

        Args:
            table: Table name
            keys: List of key values to delete
            key_column: Key column name

        Returns:
            Number of rows deleted
        """
        if not keys:
            return 0

        formatted_keys = ", ".join(self._format_value(k) for k in keys)
        self.exec(f"DELETE FROM {table} WHERE {key_column} IN ({formatted_keys})")
        return len(keys)

    # =========================================================================
    # Schema Introspection
    # =========================================================================

    def get_columns(self, table: str, database: Optional[str] = None) -> QueryResult:
        """
        Get column information for a table.

        Args:
            table: Table name
            database: Database name

        Returns:
            QueryResult with column_name, data_type, is_nullable, column_default
        """
        db = database or self._config.database
        if "." in table:
            db, table = table.split(".", 1)
        return self.query(f"DESCRIBE {db}.{table}" if db else f"DESCRIBE {table}")

    def get_indexes(self, table: str, database: Optional[str] = None) -> QueryResult:
        """
        Get index information for a table.

        Args:
            table: Table name
            database: Database name

        Returns:
            QueryResult with index information
        """
        db = database or self._config.database
        if "." in table:
            db, table = table.split(".", 1)
        full_name = f"{db}.{table}" if db else table
        return self.query(f"SHOW INDEXES FROM {full_name}")

    def get_constraints(self, table: str, database: Optional[str] = None) -> QueryResult:
        """
        Get constraint information for a table.

        Args:
            table: Table name
            database: Database name

        Returns:
            QueryResult with constraint information
        """
        db = database or self._config.database
        if "." in table:
            db, table = table.split(".", 1)
        full_name = f"{db}.{table}" if db else table
        return self.query(f"SHOW CONSTRAINTS FROM {full_name}")

    def get_table_stats(self, table: str, database: Optional[str] = None) -> QueryResult:
        """
        Get statistics for a table.

        Args:
            table: Table name
            database: Database name

        Returns:
            QueryResult with row count, size, etc.
        """
        db = database or self._config.database
        if "." in table:
            db, table = table.split(".", 1)
        full_name = f"{db}.{table}" if db else table
        return self.query(f"SHOW TABLE STATUS LIKE '{full_name}'")

    def get_schema(self, database: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get complete schema information for a database.

        Args:
            database: Database name

        Returns:
            Dictionary mapping table names to their column definitions
        """
        db = database or self._config.database
        tables = self.list_tables(db)
        schema = {}
        for table_info in tables:
            full_name = f"{table_info.database}.{table_info.name}"
            cols = self.get_columns(full_name)
            schema[full_name] = cols.rows
        return schema

    # =========================================================================
    # Array Functions
    # =========================================================================

    def array_contains(
        self,
        table: str,
        array_column: str,
        value: Any,
        *,
        select_columns: Optional[List[str]] = None,
    ) -> QueryResult:
        """
        Find rows where array column contains a value.

        Args:
            table: Table name
            array_column: Array column name
            value: Value to search for
            select_columns: Columns to return
        """
        cols = select_columns or ["*"]
        select_list = ", ".join(cols)
        formatted_value = self._format_value(value)
        return self.query(f"SELECT {select_list} FROM {table} WHERE {formatted_value} = ANY({array_column})")

    def array_overlap(
        self,
        table: str,
        array_column: str,
        values: List[Any],
        *,
        select_columns: Optional[List[str]] = None,
    ) -> QueryResult:
        """
        Find rows where array column overlaps with given values.

        Args:
            table: Table name
            array_column: Array column name
            values: Values to check for overlap
            select_columns: Columns to return
        """
        cols = select_columns or ["*"]
        select_list = ", ".join(cols)
        formatted_array = "ARRAY[" + ", ".join(self._format_value(v) for v in values) + "]"
        return self.query(f"SELECT {select_list} FROM {table} WHERE {array_column} && {formatted_array}")

    # =========================================================================
    # Async Pub/Sub Notifications
    # =========================================================================

    def poll_notifications(self, timeout: float = 0) -> List[Dict[str, Any]]:
        """
        Poll for pending notifications.

        Args:
            timeout: Max seconds to wait (0 = non-blocking)

        Returns:
            List of notification dicts with 'channel' and 'payload' keys

        Example:
            client.listen("events")
            while True:
                notifications = client.poll_notifications(timeout=1.0)
                for n in notifications:
                    print(f"Channel: {n['channel']}, Payload: {n['payload']}")
        """
        # Send a simple query to trigger notification delivery
        result = self.query("SELECT 1 AS ping")
        # Check for notifications in response metadata
        # Note: Actual notification handling depends on server protocol
        return []  # Placeholder - actual implementation depends on protocol

    # Add methods to Client class
    Client.declare_cursor = declare_cursor
    Client.fetch_cursor = fetch_cursor
    Client.move_cursor = move_cursor
    Client.close_cursor = close_cursor
    Client.lo_create = lo_create
    Client.lo_open = lo_open
    Client.lo_write = lo_write
    Client.lo_read = lo_read
    Client.lo_seek = lo_seek
    Client.lo_tell = lo_tell
    Client.lo_close = lo_close
    Client.lo_unlink = lo_unlink
    Client.lo_import = lo_import
    Client.lo_export = lo_export
    Client.advisory_lock = advisory_lock
    Client.advisory_lock_try = advisory_lock_try
    Client.advisory_unlock = advisory_unlock
    Client.advisory_unlock_all = advisory_unlock_all
    Client.advisory_xact_lock = advisory_xact_lock
    Client.fts_search = fts_search
    Client.fts_highlight = fts_highlight
    Client.geo_distance = geo_distance
    Client.geo_contains = geo_contains
    Client.geo_nearest = geo_nearest
    Client.query_as_of = query_as_of
    Client.query_between = query_between
    Client.batch_insert = batch_insert
    Client.batch_update = batch_update
    Client.batch_upsert = batch_upsert
    Client.batch_delete = batch_delete
    Client.get_columns = get_columns
    Client.get_indexes = get_indexes
    Client.get_constraints = get_constraints
    Client.get_table_stats = get_table_stats
    Client.get_schema = get_schema
    Client.array_contains = array_contains
    Client.array_overlap = array_overlap
    Client.poll_notifications = poll_notifications


# Initialize additional methods
_add_client_methods()
