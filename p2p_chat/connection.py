
from __future__ import annotations

import json
import socket
import threading
import time

from message import Message
from peer import Peer as BasePeer


BUFFER_SIZE = 4096
ENCODING = "utf-8"

HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 15


class Peer(BasePeer):
    """
    Extended Peer class that improves TCP connection handling.

    The original Peer class in peer.py remains unchanged.

    This class adds:
    - correct TCP message framing
    - one receiver thread per connection
    - thread-safe sending
    - heartbeat messages
    - connection timeout detection
    - clean connection removal
    """

    def __init__(self, username: str, host: str, port: int):
        super().__init__(username, host, port)

        self._running = True
        self._listener_socket: socket.socket | None = None

        # Protects shared connection data.
        self._connections_lock = threading.RLock()

        # Prevents multiple threads from writing to the same socket
        # at the same time.
        self._send_locks: dict[
            socket.socket,
            threading.Lock
        ] = {}

        # Stores the time at which data was last received.
        self._last_seen: dict[
            socket.socket,
            float
        ] = {}

        # Stores the address of outgoing connections.
        self._outgoing_addresses: dict[
            socket.socket,
            tuple[str, int]
        ] = {}

    def start_listener(self) -> None:
        """
        Starts the TCP server side of this peer.

        Every peer can:
        - accept incoming connections
        - create outgoing connections
        """

        listener_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        listener_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        listener_socket.bind((self.host, self.port))
        listener_socket.listen()

        # Allows the loop to periodically check whether the peer
        # should stop.
        listener_socket.settimeout(1.0)

        self._listener_socket = listener_socket

        print(
            f"[STARTED] Peer {self.username} is running on "
            f"{self.host}:{self.port}"
        )
        print(f"[PEER ID] {self.peer_id}")

        while self._running:
            try:
                connection, address = listener_socket.accept()

            except socket.timeout:
                continue

            except OSError:
                break

            print(
                f"[CONNECTED] Incoming connection from "
                f"{address[0]}:{address[1]}"
            )

            self._register_connection(
                connection=connection,
                outgoing_address=None
            )

    def connect_to_peer(
        self,
        peer_host: str,
        peer_port: int
    ) -> None:
        """
        Creates an outgoing TCP connection to another peer.

        discovery.py can continue to call this method.
        """

        peer_port = int(peer_port)
        peer_address = (peer_host, peer_port)

        # Do not connect to this peer itself.
        if (
            peer_host == self.host
            and peer_port == self.port
        ):
            return

        with self._connections_lock:
            if peer_address in self.known_peers:
                return

            # Reserve the address before connecting so that two
            # discovery events do not create duplicate connections.
            self.known_peers.add(peer_address)

        try:
            peer_socket = socket.create_connection(
                peer_address,
                timeout=5
            )

            # Return to blocking mode after connecting.
            peer_socket.settimeout(None)

            print(
                f"[CONNECTED] Connected to peer "
                f"{peer_host}:{peer_port}"
            )

            self._register_connection(
                connection=peer_socket,
                outgoing_address=peer_address
            )

        except OSError as error:
            with self._connections_lock:
                self.known_peers.discard(peer_address)

            print(
                f"[ERROR] Could not connect to "
                f"{peer_host}:{peer_port}: {error}"
            )

    def _register_connection(
        self,
        connection: socket.socket,
        outgoing_address: tuple[str, int] | None
    ) -> None:
        """
        Registers a socket and starts its receiver and heartbeat
        threads.
        """

        try:
            connection.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_KEEPALIVE,
                1
            )
        except OSError:
            pass

        with self._connections_lock:
            self.connections.append(connection)
            self._send_locks[connection] = threading.Lock()
            self._last_seen[connection] = time.monotonic()

            if outgoing_address is not None:
                self._outgoing_addresses[connection] = (
                    outgoing_address
                )

        receiver_thread = threading.Thread(
            target=self.handle_connection,
            args=(connection,),
            daemon=True
        )
        receiver_thread.start()

        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(connection,),
            daemon=True
        )
        heartbeat_thread.start()

    def handle_connection(
        self,
        connection: socket.socket
    ) -> None:
        """
        Receives data from one TCP connection.

        TCP is a byte stream. One recv() call does not always contain
        exactly one message.

        A message may be split across several recv() calls, or several
        messages may arrive together.

        Therefore, every JSON packet ends with a newline character.
        """

        buffer = bytearray()

        try:
            while self._running:
                received_data = connection.recv(BUFFER_SIZE)

                if not received_data:
                    break

                with self._connections_lock:
                    self._last_seen[connection] = time.monotonic()

                buffer.extend(received_data)

                while True:
                    newline_position = buffer.find(b"\n")

                    if newline_position == -1:
                        break

                    complete_packet = bytes(
                        buffer[:newline_position]
                    )

                    del buffer[:newline_position + 1]

                    if not complete_packet.strip():
                        continue

                    self._handle_packet(complete_packet)

        except (
            ConnectionResetError,
            ConnectionAbortedError,
            BrokenPipeError,
            OSError
        ) as error:
            if self._running:
                print(
                    f"\n[CONNECTION ERROR] "
                    f"Connection closed: {error}"
                )

        finally:
            self._remove_connection(connection)

    def _handle_packet(self, data: bytes) -> None:
        """
        Processes one complete JSON packet.
        """

        try:
            packet = json.loads(data.decode(ENCODING))
            packet_type = packet.get("type")

            if packet_type == "HEARTBEAT":
                # _last_seen has already been updated.
                return

            if packet_type == "CHAT_MESSAGE":
                message_data = packet["message"]

                message = Message.from_json(
                    json.dumps(message_data)
                )

                print(
                    f"\n[{message.sender_name}]: "
                    f"{message.text}"
                )
                print("> ", end="", flush=True)
                return

            # Also accept the original message format used by
            # your existing peer.py.
            required_fields = {
                "sender_id",
                "sender_name",
                "text",
                "timestamp"
            }

            if required_fields.issubset(packet.keys()):
                message = Message.from_json(
                    json.dumps(packet)
                )

                print(
                    f"\n[{message.sender_name}]: "
                    f"{message.text}"
                )
                print("> ", end="", flush=True)
                return

            print(
                f"[WARNING] Unknown packet received: {packet}"
            )

        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError
        ) as error:
            print(
                f"[MESSAGE ERROR] Invalid message: {error}"
            )

    def send_message(self, text: str) -> None:
        """
        Sends one chat message to all connected peers.
        """

        message = Message(
            sender_id=self.peer_id,
            sender_name=self.username,
            text=text,
            timestamp=time.time()
        )

        packet = {
            "type": "CHAT_MESSAGE",
            "message": json.loads(message.to_json())
        }

        with self._connections_lock:
            active_connections = list(self.connections)

        if not active_connections:
            print("[INFO] No peer is currently connected.")
            return

        for connection in active_connections:
            try:
                self._send_packet(
                    connection=connection,
                    packet=packet
                )

            except (
                OSError,
                ConnectionError,
                BrokenPipeError
            ) as error:
                print(
                    f"[SEND ERROR] Message could not be sent: "
                    f"{error}"
                )

                self._remove_connection(connection)

    def _send_packet(
        self,
        connection: socket.socket,
        packet: dict
    ) -> None:
        """
        Sends one JSON packet through a TCP connection.

        The newline character separates packets in the TCP stream.
        """

        encoded_packet = (
            json.dumps(
                packet,
                ensure_ascii=False,
                separators=(",", ":")
            )
            + "\n"
        ).encode(ENCODING)

        with self._connections_lock:
            send_lock = self._send_locks.get(connection)

        if send_lock is None:
            raise ConnectionError(
                "The connection is no longer active."
            )

        with send_lock:
            connection.sendall(encoded_packet)

    def _heartbeat_loop(
        self,
        connection: socket.socket
    ) -> None:
        """
        Sends heartbeat packets at regular intervals.

        If no data is received for HEARTBEAT_TIMEOUT seconds, the
        peer is treated as disconnected.
        """

        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)

            with self._connections_lock:
                if connection not in self.connections:
                    return

                last_seen = self._last_seen.get(
                    connection,
                    time.monotonic()
                )

            elapsed_time = time.monotonic() - last_seen

            if elapsed_time > HEARTBEAT_TIMEOUT:
                print(
                    "\n[TIMEOUT] A peer is no longer responding."
                )

                self._remove_connection(connection)
                return

            try:
                self._send_packet(
                    connection=connection,
                    packet={
                        "type": "HEARTBEAT",
                        "timestamp": time.time()
                    }
                )

            except (
                OSError,
                ConnectionError,
                BrokenPipeError
            ):
                self._remove_connection(connection)
                return

    def _remove_connection(
        self,
        connection: socket.socket
    ) -> None:
        """
        Safely removes and closes one connection.
        """

        with self._connections_lock:
            was_connected = connection in self.connections

            if was_connected:
                self.connections.remove(connection)

            self._send_locks.pop(connection, None)
            self._last_seen.pop(connection, None)

            outgoing_address = self._outgoing_addresses.pop(
                connection,
                None
            )

            if outgoing_address is not None:
                self.known_peers.discard(outgoing_address)

        if not was_connected:
            return

        try:
            connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        try:
            connection.close()
        except OSError:
            pass

        print("\n[DISCONNECTED] Connection closed.")
        print("> ", end="", flush=True)

    def stop(self) -> None:
        """
        Stops the listener and closes all peer connections.
        """

        self._running = False

        if self._listener_socket is not None:
            try:
                self._listener_socket.close()
            except OSError:
                pass

        with self._connections_lock:
            connections = list(self.connections)

        for connection in connections:
            self._remove_connection(connection)