from __future__ import annotations

import socket
import threading
import time
from collections.abc import Callable
from typing import Any


HEARTBEAT_INTERVAL = 2.0  # How often (seconds) heartbeats are sent to active connections
HEARTBEAT_TIMEOUT = 6.0   # How long (seconds) without any sign of life before a connection is considered dead


class HeartbeatManager:
    # Generic fault-detection component: tracks per-connection "last seen" times
    # and periodically sends heartbeats, independent of Bully/discovery logic.

    def __init__(
        self,
        send_packet: Callable[[socket.socket, dict[str, Any]], None],
        on_timeout: Callable[[socket.socket], None],
        interval: float = HEARTBEAT_INTERVAL,
        timeout: float = HEARTBEAT_TIMEOUT
    ):
        self.send_packet = send_packet  # Callback used to actually send a HEARTBEAT packet on a connection
        self.on_timeout = on_timeout    # Callback invoked when a connection is considered dead

        self.interval = float(interval)
        self.timeout = float(timeout)

        self._last_seen: dict[socket.socket, float] = {}
        self._active: dict[socket.socket, bool] = {}  # Whether we actively send heartbeats on this connection (only true for the leader's connections)
        self._lock = threading.RLock()

        self._running = False
        self._monitor_thread: threading.Thread | None = None

    def start(self) -> None:
        # Starts the background monitoring loop exactly once
        with self._lock:
            if self._running:
                return

            self._running = True

        self._monitor_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True
        )
        self._monitor_thread.start()

    def register_connection(
        self,
        connection: socket.socket,
        active: bool = False
    ) -> None:
        # Starts tracking a new connection with the current time as its last-seen baseline
        with self._lock:
            self._last_seen[connection] = time.monotonic()
            self._active[connection] = active

    def set_active(
        self,
        connection: socket.socket,
        active: bool
    ) -> None:
        # Toggles whether we proactively send heartbeats on this connection
        # (used when the leader role changes, see Peer.update_heartbeat_roles)
        with self._lock:
            if connection not in self._last_seen:
                self._last_seen[connection] = time.monotonic()

            self._active[connection] = active

    def unregister_connection(
        self,
        connection: socket.socket
    ) -> None:
        # Stops tracking a connection, e.g. after it has been closed
        with self._lock:
            self._last_seen.pop(connection, None)
            self._active.pop(connection, None)

    def mark_alive(
        self,
        connection: socket.socket
    ) -> None:
        # Refreshes the last-seen timestamp; called whenever any data arrives on the connection,
        # not just heartbeat packets, so normal traffic also counts as a liveness signal
        with self._lock:
            if connection in self._last_seen:
                self._last_seen[connection] = time.monotonic()

    def _heartbeat_loop(self) -> None:
        # Background thread: periodically checks all tracked connections for timeouts
        # and sends heartbeats on the ones marked as active
        while True:
            time.sleep(self.interval)

            with self._lock:
                if not self._running:
                    return

                connections = [
                    (connection, last_seen, self._active.get(connection, False))
                    for connection, last_seen in self._last_seen.items()
                ]

            current_time = time.monotonic()

            for connection, last_seen, is_active in connections:
                elapsed_time = current_time - last_seen

                if elapsed_time > self.timeout:
                    # No activity for too long -> treat the peer as failed
                    print(
                        f"\n[HEARTBEAT TIMEOUT] "
                        f"No response for {elapsed_time:.1f} seconds."
                    )

                    self.unregister_connection(connection)

                    try:
                        self.on_timeout(connection)
                    except Exception as error:
                        print(
                            f"[HEARTBEAT ERROR] "
                            f"Timeout handler failed: {error}"
                        )

                    continue

                if not is_active:
                    # Passive connections rely on the other side to send heartbeats;
                    # we only monitor for timeout, we don't send anything ourselves
                    continue

                try:
                    self.send_packet(
                        connection,
                        {
                            "type": "HEARTBEAT",

                        }
                    )

                except (
                    OSError,
                    ConnectionError,
                    BrokenPipeError
                ):
                    # Sending failed -> connection is effectively dead, treat like a timeout
                    print(
                        "\n[HEARTBEAT ERROR] "
                        "Heartbeat could not be sent."
                    )

                    self.unregister_connection(connection)

                    try:
                        self.on_timeout(connection)
                    except Exception as error:
                        print(
                            f"[HEARTBEAT ERROR] "
                            f"Timeout handler failed: {error}"
                        )

    def stop(self) -> None:
        # Stops the monitoring loop and clears all tracked state
        with self._lock:
            self._running = False
            self._last_seen.clear()
            self._active.clear()