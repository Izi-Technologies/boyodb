"""
Pub/Sub and CDC subscriptions for BoyoDB.

Provides real-time data streaming capabilities.
"""

import json
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .client import Client


class ChangeType(Enum):
    """CDC change types."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    TRUNCATE = "truncate"


@dataclass
class ChangeEvent:
    """CDC change event."""
    table: str
    change_type: ChangeType
    before: Optional[Dict[str, Any]]  # For UPDATE/DELETE
    after: Optional[Dict[str, Any]]   # For INSERT/UPDATE
    timestamp: int
    transaction_id: Optional[str]
    sequence: int


@dataclass
class Message:
    """Pub/Sub message."""
    channel: str
    payload: Any
    timestamp: int


class Subscriber:
    """
    Pub/Sub subscriber for real-time messaging.

    Usage:
        def on_message(msg):
            print(f"Received: {msg.payload}")

        sub = Subscriber(client, ["events", "alerts"], on_message)
        sub.start()
        # ...
        sub.stop()
    """

    def __init__(
        self,
        client: "Client",
        channels: List[str],
        callback: Callable[[Message], None],
        error_callback: Optional[Callable[[Exception], None]] = None,
    ):
        self._client = client
        self._channels = channels
        self._callback = callback
        self._error_callback = error_callback
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start listening for messages."""
        if self._running:
            return

        # Subscribe to channels
        for channel in self._channels:
            self._client.exec(f"LISTEN {channel}")

        self._running = True
        self._thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop listening and unsubscribe."""
        self._running = False

        # Unsubscribe from channels
        for channel in self._channels:
            try:
                self._client.exec(f"UNLISTEN {channel}")
            except Exception:
                pass

        if self._thread:
            self._thread.join(timeout=5.0)

    def _listen_loop(self) -> None:
        """Background listening loop."""
        while self._running:
            try:
                # Poll for notifications
                result = self._client.query("SELECT pg_notification_poll()")
                for row in result:
                    if row.get("channel") and row.get("payload"):
                        msg = Message(
                            channel=row["channel"],
                            payload=self._parse_payload(row["payload"]),
                            timestamp=row.get("timestamp", int(time.time() * 1000)),
                        )
                        self._callback(msg)

            except Exception as e:
                if self._error_callback:
                    self._error_callback(e)

            time.sleep(0.1)  # Poll interval

    @staticmethod
    def _parse_payload(payload: str) -> Any:
        """Try to parse payload as JSON."""
        try:
            return json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            return payload


class CDCSubscriber:
    """
    CDC (Change Data Capture) subscriber for table changes.

    Usage:
        def on_change(event):
            print(f"{event.change_type}: {event.after}")

        cdc = CDCSubscriber(client, "mydb.users", on_change)
        cdc.start()
        # ...
        cdc.stop()
    """

    def __init__(
        self,
        client: "Client",
        table: str,
        callback: Callable[[ChangeEvent], None],
        error_callback: Optional[Callable[[Exception], None]] = None,
        include_before: bool = True,
        change_types: Optional[List[ChangeType]] = None,
    ):
        self._client = client
        self._table = table
        self._callback = callback
        self._error_callback = error_callback
        self._include_before = include_before
        self._change_types = change_types or list(ChangeType)
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_sequence = 0
        self._slot_name: Optional[str] = None

    def start(self) -> None:
        """Start receiving CDC events."""
        if self._running:
            return

        # Create replication slot
        self._slot_name = f"cdc_{self._table.replace('.', '_')}_{int(time.time())}"

        try:
            self._client.exec(
                f"SELECT pg_create_logical_replication_slot('{self._slot_name}', 'boyodb_cdc')"
            )
        except Exception:
            # Slot might already exist or be managed differently
            pass

        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop receiving CDC events."""
        self._running = False

        # Drop replication slot
        if self._slot_name:
            try:
                self._client.exec(
                    f"SELECT pg_drop_replication_slot('{self._slot_name}')"
                )
            except Exception:
                pass

        if self._thread:
            self._thread.join(timeout=5.0)

    def _poll_loop(self) -> None:
        """Background polling loop."""
        while self._running:
            try:
                # Query for changes
                result = self._client.query(
                    f"""
                    SELECT * FROM pg_logical_slot_get_changes(
                        '{self._slot_name}',
                        NULL,
                        1000,
                        'table', '{self._table}',
                        'include_before', '{str(self._include_before).lower()}'
                    )
                    """
                )

                for row in result:
                    event = self._parse_change(row)
                    if event and event.change_type in self._change_types:
                        self._callback(event)
                        self._last_sequence = event.sequence

            except Exception as e:
                if self._error_callback:
                    self._error_callback(e)

            time.sleep(0.1)  # Poll interval

    def _parse_change(self, row: Dict[str, Any]) -> Optional[ChangeEvent]:
        """Parse a change row into a ChangeEvent."""
        try:
            data = row.get("data") or row
            if isinstance(data, str):
                data = json.loads(data)

            change_type_str = data.get("op", data.get("change_type", "insert"))
            change_type = ChangeType(change_type_str.lower())

            return ChangeEvent(
                table=data.get("table", self._table),
                change_type=change_type,
                before=data.get("before"),
                after=data.get("after"),
                timestamp=data.get("ts", int(time.time() * 1000)),
                transaction_id=data.get("txid"),
                sequence=data.get("seq", self._last_sequence + 1),
            )
        except Exception:
            return None


class Publisher:
    """
    Pub/Sub publisher for sending messages.

    Usage:
        pub = Publisher(client)
        pub.publish("events", {"type": "user_signup", "user_id": 123})
    """

    def __init__(self, client: "Client"):
        self._client = client

    def publish(self, channel: str, payload: Any) -> None:
        """Publish a message to a channel."""
        if isinstance(payload, (dict, list)):
            payload_str = json.dumps(payload).replace("'", "''")
        else:
            payload_str = str(payload).replace("'", "''")

        self._client.exec(f"NOTIFY {channel}, '{payload_str}'")

    def publish_many(self, channel: str, payloads: List[Any]) -> None:
        """Publish multiple messages to a channel."""
        for payload in payloads:
            self.publish(channel, payload)
