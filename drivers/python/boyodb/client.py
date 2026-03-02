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
