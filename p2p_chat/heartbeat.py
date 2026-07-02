from __future__ import annotations

import socket
import threading
import time
from collections.abc import Callable
from typing import Any


HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0


class HeartbeatManager:

    def __init__(
        self,
        send_packet: Callable[[socket.socket, dict[str, Any]], None],
        on_timeout: Callable[[socket.socket], None],
        interval: float = HEARTBEAT_INTERVAL,
        timeout: float = HEARTBEAT_TIMEOUT
    ):
        self.send_packet = send_packet
        self.on_timeout = on_timeout

        self.interval = float(interval)
        self.timeout = float(timeout)

        self._last_seen: dict[socket.socket, float] = {}
        self._active: dict[socket.socket, bool] = {}
        self._lock = threading.RLock()

        self._running = False
        self._monitor_thread: threading.Thread | None = None

    def start(self) -> None:

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


        with self._lock:
            self._last_seen[connection] = time.monotonic()
            self._active[connection] = active

    def set_active(
        self,
        connection: socket.socket,
        active: bool
    ) -> None:

        with self._lock:
            if connection not in self._last_seen:
                self._last_seen[connection] = time.monotonic()

            self._active[connection] = active

    def unregister_connection(
        self,
        connection: socket.socket
    ) -> None:

        with self._lock:
            self._last_seen.pop(connection, None)
            self._active.pop(connection, None)

    def mark_alive(
        self,
        connection: socket.socket
    ) -> None:

        with self._lock:
            if connection in self._last_seen:
                self._last_seen[connection] = time.monotonic()

    def _heartbeat_loop(self) -> None:

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
                    continue

                try:
                    self.send_packet(
                        connection,
                        {
                            "type": "HEARTBEAT",
                            "timestamp": time.time()
                        }
                    )

                except (
                    OSError,
                    ConnectionError,
                    BrokenPipeError
                ):
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

        with self._lock:
            self._running = False
            self._last_seen.clear()
            self._active.clear()