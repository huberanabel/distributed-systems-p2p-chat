import json
import socket
import threading
import uuid

from message import Message
from heartbeat import HeartbeatManager
from election import BullyElection
from lamport_clock import LamportClock
from network import get_local_ip


MAX_PACKET_SIZE = 1_048_576
MAX_RECEIVE_BUFFER = MAX_PACKET_SIZE


class Peer:
    # Central class: manages TCP connections to other peers, dispatches
    # incoming packets, and wires together heartbeat, election, and Lamport clock.

    def __init__(self, username, host, port):
        self.peer_id = str(uuid.uuid4())
        self.username = username

        # self.host is the BIND address (usually "0.0.0.0", meaning
        # "accept connections on every local network interface").
        # It is only ever valid to pass to socket.bind() and must
        # never be told to other peers as an address to connect to.
        self.host = host
        self.port = int(port)

        # advertised_host is this device's actual, routable IP
        # address -- the one other peers can really connect to. It
        # is determined once at startup (see network_utils.py) and
        # used everywhere we announce our own address to the
        # network (currently: broadcast_peer_list's self entry).
        self.advertised_host = get_local_ip()

        # Unique process ID, independent of the port used.
        self.process_id = uuid.uuid4().int & ((1 << 63) - 1)

        if self.process_id == 0:
            self.process_id = 1

        self.connections = []
        self.known_peers = set()
        self.member_ids = {self.process_id}

        self.running = True

        # Protects shared data from concurrent thread access.
        self.connection_lock = threading.RLock()

        # Stored so stop() can close the listener.
        self.listener_socket = None

        # Receive data is stored as bytes.
        self.receive_buffers = {}

        self.remote_peers = {}

        # Keeps usernames even after a peer disconnects,
        # so they can still be shown to other peers.
        self._known_usernames = {}
        self.connection_addresses = {}
        self.outgoing_connections = set()

        # Prevents multiple threads from writing to the
        # same socket at the same time.
        self.send_locks = {}

        self.lamport_clock = LamportClock()
        self.lamport_lock = threading.Lock()

        self.heartbeat = HeartbeatManager(
            send_packet=self.send_control_packet,
            on_timeout=self.handle_peer_timeout
        )

        self.election = BullyElection(
            process_id=self.process_id,
            get_members=self.get_member_ids,
            broadcast_packet=self.broadcast_control_packet,
            on_leader_change=self.on_leader_change,
            get_username=self.get_username_for_process_id
        )

        self.heartbeat.start()

    # ------------------------------------------------------------
    # Register connections
    # ------------------------------------------------------------

    def _register_connection(
        self,
        connection,
        peer_address=None,
        outgoing=False
    ):
        # Adds a new socket to all the bookkeeping structures
        # (buffers, locks, heartbeat tracking) needed to use it
        with self.connection_lock:
            if connection in self.connections:
                return

            self.connections.append(connection)
            self.receive_buffers[connection] = b""
            self.send_locks[connection] = threading.Lock()

            if peer_address is not None:
                self.connection_addresses[connection] = peer_address
                self.known_peers.add(peer_address)

            if outgoing:
                self.outgoing_connections.add(connection)

        self.heartbeat.register_connection(
            connection,
            active=False
        )

    # ------------------------------------------------------------
    # Listener
    # ------------------------------------------------------------

    def start_listener(self):
        # Runs in its own thread: accepts incoming TCP connections from other peers
        listener_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        listener_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        listener_socket.settimeout(1.0)

        self.listener_socket = listener_socket

        try:
            listener_socket.bind(
                (self.host, self.port)
            )

            listener_socket.listen()

            print(
                f"[STARTED] Peer {self.username} "
                f"is running on {self.host}:{self.port}"
            )

            print(
                f"[REACHABLE AT] Other devices can reach this "
                f"peer at {self.advertised_host}:{self.port}"
            )

            print(f"[PEER ID] {self.peer_id}")
            print(f"[PROCESS ID] {self.process_id}")

            while self.running:
                try:
                    connection, address = (
                        listener_socket.accept()
                    )

                except socket.timeout:
                    # Timeout just lets the loop re-check self.running periodically
                    continue

                except OSError:
                    break

                self._register_connection(connection)

                print(
                    f"[CONNECTING] Incoming connection "
                    f"from {address[0]}..."
                )

                # Each connection gets its own receive thread
                thread = threading.Thread(
                    target=self.handle_connection,
                    args=(connection,),
                    daemon=True
                )

                thread.start()

                self.send_hello(connection)
                self.update_heartbeat_roles()

        except OSError as error:
            if self.running:
                print(
                    f"[START ERROR] Could not listen on "
                    f"{self.host}:{self.port}: {error}"
                )

        finally:
            try:
                listener_socket.close()

            except OSError:
                pass

            if self.listener_socket is listener_socket:
                self.listener_socket = None

    # ------------------------------------------------------------
    # Connect to a peer
    # ------------------------------------------------------------

    def connect_to_peer(self, peer_host, peer_port):
        # Actively opens an outgoing TCP connection to another peer
        peer_port = int(peer_port)
        peer_address = (peer_host, peer_port)

        if (
            peer_port == self.port
            and peer_host in {
                self.host,
                self.advertised_host,
                "127.0.0.1",
                "localhost",
                "0.0.0.0"
            }
        ):
            # Avoid connecting to ourselves
            return

        with self.connection_lock:
            if peer_address in self.known_peers:
                return

            # Reserve the address temporarily to avoid duplicate connection attempts.
            self.known_peers.add(peer_address)

        peer_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        try:
            peer_socket.settimeout(5)
            peer_socket.connect(peer_address)
            peer_socket.settimeout(None)

            self._register_connection(
                peer_socket,
                peer_address=peer_address,
                outgoing=True
            )

            print(
                f"[CONNECTING] Connection to "
                f"{peer_host}:{peer_port} established, "
                f"waiting for handshake..."
            )

            thread = threading.Thread(
                target=self.handle_connection,
                args=(peer_socket,),
                daemon=True
            )

            thread.start()

            self.send_hello(peer_socket)
            self.update_heartbeat_roles()

        except Exception as error:
            # Connection failed -> release the reserved address again
            with self.connection_lock:
                self.known_peers.discard(peer_address)

            try:
                peer_socket.close()

            except OSError:
                pass

            print(
                f"[ERROR] Could not connect to peer "
                f"{peer_host}:{peer_port}: {error}"
            )

    # ------------------------------------------------------------
    # Send HELLO
    # ------------------------------------------------------------

    def send_hello(self, connection):
        # HELLO is the handshake packet exchanged right after a TCP
        # connection is established, telling the other side who we are
        packet = {
            "type": "HELLO",
            "peer_id": self.peer_id,
            "username": self.username,
            "port": self.port,
            "process_id": self.process_id
        }

        try:
            self.send_control_packet(
                connection,
                packet
            )

        except Exception as error:
            print(
                f"[ERROR] Could not send HELLO packet: "
                f"{error}"
            )

            self.remove_connection(connection)

    # ------------------------------------------------------------
    # Receive data
    # ------------------------------------------------------------

    def handle_connection(self, connection):
        # Per-connection receive loop: reads raw bytes, splits them into
        # newline-delimited JSON packets, and dispatches each one
        while self.running:
            try:
                data = connection.recv(4096)

                if not data:
                    # Peer closed the connection
                    break

                self.heartbeat.mark_alive(connection)

                with self.connection_lock:
                    current_buffer = self.receive_buffers.get(
                        connection
                    )

                    if current_buffer is None:
                        break

                    current_buffer += data

                    if len(current_buffer) > MAX_RECEIVE_BUFFER:
                        # Guards against a malicious/buggy peer flooding us without newlines
                        raise ValueError(
                            "Receive buffer is too large."
                        )

                    packet_lines = []

                    # Packets are newline-delimited; split off every complete line,
                    # keeping any leftover partial data in the buffer for next time
                    while b"\n" in current_buffer:
                        packet_bytes, current_buffer = (
                            current_buffer.split(
                                b"\n",
                                1
                            )
                        )

                        if packet_bytes.strip():
                            packet_lines.append(packet_bytes)

                    self.receive_buffers[connection] = (
                        current_buffer
                    )

                for packet_bytes in packet_lines:

                    with self.connection_lock:
                        still_connected = (
                            connection in self.connections
                        )

                    if not still_connected:
                        return

                    try:
                        packet_text = packet_bytes.decode(
                            "utf-8"
                        )

                        packet = json.loads(packet_text)

                        if not isinstance(packet, dict):
                            raise TypeError(
                                "Packet must be a JSON object."
                            )

                        self._handle_packet(
                            connection,
                            packet
                        )

                    except (
                        UnicodeDecodeError,
                        json.JSONDecodeError,
                        KeyError,
                        TypeError,
                        ValueError
                    ) as error:
                        # A single malformed packet shouldn't kill the whole connection
                        print(
                            f"[WARNING] Invalid packet ignored: "
                            f"{error}"
                        )

            except (
                OSError,
                ConnectionError,
                ValueError
            ) as error:
                if self.running:
                    print(
                        f"[ERROR] Connection ended: "
                        f"{error}"
                    )

                break

        self.remove_connection(connection)

    # ------------------------------------------------------------
    # Process a single packet
    # ------------------------------------------------------------

    def _handle_packet(self, connection, packet):
        # Routes an incoming packet to the right handler based on its "type"
        packet_type = packet.get("type")

        if packet_type == "HELLO":
            self.handle_hello(
                connection,
                packet
            )

        elif packet_type == "PEER_LIST":
            self.handle_peer_list(
                connection,
                packet
            )

        elif packet_type == "HEARTBEAT":
            # Reply to a heartbeat so the sender knows we're still alive
            self.send_control_packet(
                connection,
                {
                    "type": "HEARTBEAT_ACK"
                }
            )

        elif packet_type == "HEARTBEAT_ACK":
            # No action needed; receiving any data already marks the connection alive
            return

        elif packet_type in {
            "ELECTION",
            "OK",
            "COORDINATOR"
        }:
            # Bully algorithm control packets are forwarded to the election module,
            # but only after verifying the sender_id matches the connection's HELLO identity
            remote_peer = self._get_remote_peer(
                connection
            )

            if remote_peer is None:
                raise ValueError(
                    "Control packet received before HELLO."
                )

            sender_id = int(
                packet.get("sender_id")
            )

            if sender_id != remote_peer["process_id"]:
                raise ValueError(
                    "Control packet has a false sender_id."
                )

            self.election.handle_packet(packet)

        elif packet_type == "CHAT_MESSAGE":
            remote_peer = self._get_remote_peer(
                connection
            )

            if remote_peer is None:
                raise ValueError(
                    "Chat message received before HELLO."
                )

            message_data = packet["message"]

            message = Message(
                sender_id=message_data["sender_id"],
                sender_name=message_data["sender_name"],
                text=message_data["text"],
                lamport_time=int(
                    message_data["lamport_time"]
                )
            )

            # Verify the message actually came from the peer on this connection
            # (prevents spoofing another peer's identity)
            if message.sender_id != remote_peer["peer_id"]:
                raise ValueError(
                    "Chat message has a false sender_id."
                )

            if message.sender_name != remote_peer["username"]:
                raise ValueError(
                    "Chat message has a false sender_name."
                )

            # Update our Lamport clock based on the received timestamp
            with self.lamport_lock:
                self.lamport_clock.receive_event(
                    message.lamport_time
                )

                logical_time = (
                    self.lamport_clock.get_time()
                )

            print(
                f"\n[L={logical_time}] "
                f"[{message.sender_name}]: "
                f"{message.text}"
            )

            print(
                "> ",
                end="",
                flush=True
            )

        else:
            print(
                f"[WARNING] Unknown packet: "
                f"{packet}"
            )

    # ------------------------------------------------------------
    # Retrieve peer information
    # ------------------------------------------------------------

    def _get_remote_peer(self, connection):
        # Returns a copy of the stored HELLO info for a connection (or None if not yet handshaked)
        with self.connection_lock:
            information = self.remote_peers.get(
                connection
            )

            if information is None:
                return None

            return dict(information)

    # ------------------------------------------------------------
    # Compare duplicate connections
    # ------------------------------------------------------------

    def _connection_rank(self, connection):
        # Produces a deterministic, comparable "rank" for a connection based on
        # its socket endpoints, used as a tie-breaker when two peers connect to
        # each other simultaneously and end up with duplicate connections
        try:
            endpoints = [
                connection.getsockname(),
                connection.getpeername()
            ]

            return tuple(sorted(endpoints))

        except OSError:
            return (
                ("~", 65536),
                ("~", 65536)
            )

    # ------------------------------------------------------------
    # Process HELLO
    # ------------------------------------------------------------

    def handle_hello(self, connection, packet):
        # Processes the handshake packet from a peer, including detecting and
        # resolving duplicate connections that can arise when both sides dial
        # each other around the same time.
        process_id = int(packet["process_id"])
        peer_port = int(packet["port"])

        if process_id == self.process_id:
            raise ValueError(
                "Another peer uses the same process ID."
            )

        try:
            real_host = connection.getpeername()[0]

        except OSError:
            real_host = None

        peer_information = {
            "peer_id": packet["peer_id"],
            "username": packet["username"],
            "host": real_host,
            "port": peer_port,
            "process_id": process_id
        }

        with self.connection_lock:
            # Check whether we already have a different connection to the same process ID
            existing_connection = next(
                (
                    current_connection
                    for current_connection, information
                    in self.remote_peers.items()
                    if (
                        information["process_id"]
                        == process_id
                        and current_connection
                        is not connection
                    )
                ),
                None
            )

            new_is_outgoing = (
                connection
                in self.outgoing_connections
            )

            existing_is_outgoing = (
                existing_connection
                in self.outgoing_connections
                if existing_connection is not None
                else False
            )

        if existing_connection is not None:
            # Duplicate connection detected: decide deterministically which one to keep.
            # Rule of thumb: the connection whose direction matches "lower ID dials
            # out to higher ID" is preferred; if both/neither match, fall back to
            # a stable rank comparison so both sides make the same decision.
            desired_outgoing = (
                self.process_id < process_id
            )

            if (
                new_is_outgoing == desired_outgoing
                and existing_is_outgoing
                != desired_outgoing
            ):
                connection_to_remove = (
                    existing_connection
                )

            elif (
                existing_is_outgoing
                == desired_outgoing
                and new_is_outgoing
                != desired_outgoing
            ):
                connection_to_remove = connection

            else:
                keep_connection = min(
                    (
                        existing_connection,
                        connection
                    ),
                    key=self._connection_rank
                )

                if keep_connection is existing_connection:
                    connection_to_remove = connection
                else:
                    connection_to_remove = (
                        existing_connection
                    )

            print(
                f"[PEER] Duplicate connection to "
                f"{peer_information['username']} detected. "
                f"Closing the redundant connection."
            )

            self.remove_connection(
                connection_to_remove
            )

            if connection_to_remove is connection:
                return

        with self.connection_lock:
            if connection not in self.connections:
                return

            self.remote_peers[connection] = (
                peer_information
            )

            self.member_ids.add(process_id)

            self._known_usernames[process_id] = (
                peer_information["username"]
            )

            if (
                connection
                not in self.connection_addresses
                and real_host is not None
            ):
                # Fills in the address for incoming connections, which
                # didn't have one recorded at accept() time
                peer_address = (
                    real_host,
                    peer_port
                )

                self.connection_addresses[
                    connection
                ] = peer_address

                self.known_peers.add(peer_address)

        print(
            f"[CONNECTED] "
            f"{peer_information['username']} is connected."
        )

        if self.is_leader():
            # Leader shares the full peer list so the new peer learns about everyone else
            self.broadcast_peer_list()

        self.update_heartbeat_roles()

    # ------------------------------------------------------------
    # Process peer list
    # ------------------------------------------------------------

    def handle_peer_list(self, connection, packet):
        # Handles a PEER_LIST packet from the leader: adopts the announced
        # leader and connects to any newly-learned peers we're not yet connected to
        try:
            leader_process_id = int(
                packet["leader_process_id"]
            )

            entries = packet.get(
                "peers",
                []
            )

            if not isinstance(entries, list):
                raise TypeError

        except (
            KeyError,
            TypeError,
            ValueError
        ):
            print(
                "[PEER_LIST ERROR] "
                "Invalid PEER_LIST packet."
            )

            return

        sender_information = self._get_remote_peer(
            connection
        )

        if (
            sender_information is None
            or sender_information["process_id"]
            != leader_process_id
        ):
            # Only trust a PEER_LIST if it actually comes from the process claiming to be leader
            print(
                "[PEER_LIST ERROR] The peer list "
                "was not sent by its claimed leader."
            )

            return

        # Note: we deliberately do NOT reject this leader claim just
        # because leader_process_id is lower than our own process
        # ID. The sender_information check above already confirmed
        # this PEER_LIST truly came from the connection belonging to
        # that process (via its HELLO handshake), so it is a
        # trustworthy, already-settled fact about the network, not
        # a competing candidacy to be judged by Bully's ID rule.
        # The leader should only ever change again once it actually
        # fails (see handle_leader_failure), not simply because a
        # peer with a higher ID happens to join later.
        if (
            self.election.get_leader()
            != leader_process_id
        ):
            self.election.set_leader(
                leader_process_id
            )

        for entry in entries:
            try:
                remote_process_id = int(
                    entry["process_id"]
                )

                remote_host = entry.get("host")

                remote_port = int(
                    entry["port"]
                )

            except (
                KeyError,
                TypeError,
                ValueError
            ):
                continue

            if remote_process_id == self.process_id:
                continue

            self.register_member(
                remote_process_id
            )

            if remote_process_id == leader_process_id:
                # Already connected to the leader (that's how we got this packet)
                continue

            if (
                self.get_connection_for_process_id(
                    remote_process_id
                )
                is not None
            ):
                # Already connected to this peer
                continue

            if (
                not remote_host
                or remote_host == "0.0.0.0"
            ):
                # Can't dial an unspecified/bind address
                continue

            if self.process_id > remote_process_id:
                # Only the lower-ID side initiates the connection, to avoid
                # both sides dialing each other and creating duplicates
                continue

            self.connect_to_peer(
                remote_host,
                remote_port
            )

        self.update_heartbeat_roles()

    # ------------------------------------------------------------
    # Send chat message
    # ------------------------------------------------------------

    def send_message(self, text):
        # Sends a chat message to all connected peers, tagging it with
        # a fresh Lamport timestamp
        with self.lamport_lock:
            lamport_time = (
                self.lamport_clock.send_event()
            )

        packet = {
            "type": "CHAT_MESSAGE",
            "message": {
                "sender_id": self.peer_id,
                "sender_name": self.username,
                "text": text,
                "lamport_time": lamport_time
            }
        }

        self.broadcast_control_packet(packet)

    # ------------------------------------------------------------
    # Send control packet
    # ------------------------------------------------------------

    def send_control_packet(
        self,
        connection,
        packet
    ):
        # Serializes and sends a single packet on one connection.
        # A trailing newline acts as the message delimiter (see handle_connection).
        encoded_packet = (
            json.dumps(
                packet,
                ensure_ascii=False
            ) + "\n"
        ).encode("utf-8")

        if len(encoded_packet) > MAX_PACKET_SIZE:
            raise ValueError(
                "Packet is too large."
            )

        with self.connection_lock:
            send_lock = self.send_locks.get(
                connection
            )

        if send_lock is None:
            raise ConnectionError(
                "Connection is no longer registered."
            )

        # Per-connection lock avoids interleaving bytes from different
        # threads writing to the same socket concurrently
        with send_lock:
            connection.sendall(encoded_packet)

    # ------------------------------------------------------------
    # Send packet to all peers
    # ------------------------------------------------------------

    def broadcast_control_packet(self, packet):
        # Sends the same packet to every currently connected peer;
        # any connection that fails to send is torn down
        with self.connection_lock:
            connections = list(
                self.connections
            )

        for connection in connections:
            try:
                self.send_control_packet(
                    connection,
                    packet
                )

            except Exception as error:
                print(
                    f"[ERROR] Could not send "
                    f"control packet: {error}"
                )

                self.remove_connection(
                    connection
                )

    # ------------------------------------------------------------
    # Send peer list
    # ------------------------------------------------------------

    def broadcast_peer_list(self):
        # Only meaningful when called by the current leader: shares the
        # full known peer directory (including ourselves) with everyone
        with self.connection_lock:
            remote_information = [
                dict(information)
                for information
                in self.remote_peers.values()
            ]

        entries = [
            {
                "peer_id": self.peer_id,
                "username": self.username,
                "host": self.advertised_host,
                "port": self.port,
                "process_id": self.process_id
            }
        ]

        for information in remote_information:
            entries.append({
                "peer_id": information["peer_id"],
                "username": information["username"],
                "host": information["host"],
                "port": information["port"],
                "process_id": information["process_id"]
            })

        packet = {
            "type": "PEER_LIST",
            "leader_process_id": self.process_id,
            "peers": entries
        }

        self.broadcast_control_packet(packet)

    # ------------------------------------------------------------
    # Member management
    # ------------------------------------------------------------

    def register_member(self, process_id):
        # Adds a process ID to the known member set, used by the Bully
        # algorithm to know who else is out there (even before a direct connection exists)
        with self.connection_lock:
            self.member_ids.add(
                int(process_id)
            )

    def get_member_ids(self):
        # Thread-safe snapshot of all known process IDs (used by BullyElection)
        with self.connection_lock:
            return set(self.member_ids)

    def get_connection_for_process_id(
        self,
        process_id
    ):
        # Looks up the socket connection belonging to a given process ID, if any
        process_id = int(process_id)

        with self.connection_lock:
            for connection, information in (
                self.remote_peers.items()
            ):
                if (
                    information["process_id"]
                    == process_id
                ):
                    return connection

        return None

    def get_username_for_process_id(
        self,
        process_id
    ):
        # Resolves a process ID to a display name, falling back to a cached
        # username even if the peer has since disconnected
        if process_id == self.process_id:
            return self.username

        with self.connection_lock:
            for information in (
                self.remote_peers.values()
            ):
                if (
                    information["process_id"]
                    == process_id
                ):
                    return information["username"]

            return self._known_usernames.get(process_id)

    # ------------------------------------------------------------
    # Election helper methods
    # ------------------------------------------------------------

    def start_election(self):
        self.election.start_election()

    def get_leader(self):
        return self.election.get_leader()

    def is_leader(self):
        leader_id = self.election.get_leader()

        return (
            leader_id is not None
            and leader_id == self.process_id
        )

    def on_leader_change(self, leader_id):
        # Called by BullyElection whenever the leader changes: heartbeat
        # roles need to be recalculated, and a new leader shares the peer list
        self.update_heartbeat_roles()

        if self.is_leader():
            self.broadcast_peer_list()

    # ------------------------------------------------------------
    # Update heartbeat roles
    # ------------------------------------------------------------

    def update_heartbeat_roles(self):
        # Decides which connections should actively send heartbeats:
        # - If there's no leader yet, everyone heartbeats everyone (safety net).
        # - If we are the leader, we heartbeat all our connections.
        # - Otherwise, we only heartbeat the leader's connection and stop
        #   tracking all others (the leader is responsible for those).
        leader_id = self.get_leader()

        with self.connection_lock:
            connections = list(
                self.connections
            )

        if leader_id is None:
            for connection in connections:
                self.heartbeat.set_active(
                    connection,
                    True
                )

            return

        if self.is_leader():
            for connection in connections:
                self.heartbeat.set_active(
                    connection,
                    True
                )

            return

        leader_connection = (
            self.get_connection_for_process_id(
                leader_id
            )
        )

        for connection in connections:
            if connection is leader_connection:
                self.heartbeat.set_active(
                    connection,
                    False
                )

            else:
                self.heartbeat.unregister_connection(
                    connection
                )

    # ------------------------------------------------------------
    # Handle timeout
    # ------------------------------------------------------------

    def handle_peer_timeout(self, connection):
        # Called by HeartbeatManager when a connection stops responding
        with self.connection_lock:
            connected = (
                connection in self.connections
            )

        if not connected:
            return

        print(
            "\n[FAULT TOLERANCE] "
            "A peer is no longer reachable."
        )

        self.remove_connection(connection)

    # ------------------------------------------------------------
    # Remove connection
    # ------------------------------------------------------------

    def remove_connection(self, connection):
        # Tears down a connection and all associated state, and reacts
        # accordingly if the failed peer turns out to have been the leader
        with self.connection_lock:
            if connection not in self.connections:
                return

            remote_peer = self.remote_peers.pop(
                connection,
                None
            )

            peer_address = (
                self.connection_addresses.pop(
                    connection,
                    None
                )
            )

            self.connections.remove(connection)

            self.receive_buffers.pop(
                connection,
                None
            )

            self.send_locks.pop(
                connection,
                None
            )

            self.outgoing_connections.discard(
                connection
            )

            if peer_address is not None:
                self.known_peers.discard(
                    peer_address
                )

            failed_process_id = None
            failed_username = "Unknown peer"

            if remote_peer is not None:
                failed_process_id = (
                    remote_peer["process_id"]
                )

                failed_username = (
                    remote_peer["username"]
                )

                # A peer can have multiple connections in edge cases;
                # only treat it as fully gone if no connection remains
                still_connected = any(
                    information["process_id"]
                    == failed_process_id
                    for information
                    in self.remote_peers.values()
                )

                if not still_connected:
                    self.member_ids.discard(
                        failed_process_id
                    )

            else:
                still_connected = False

        self.heartbeat.unregister_connection(
            connection
        )

        try:
            connection.shutdown(
                socket.SHUT_RDWR
            )

        except OSError:
            pass

        try:
            connection.close()

        except OSError:
            pass

        print(
            f"\n[DISCONNECTED] "
            f"{failed_username} disconnected."
        )

        print(
            "> ",
            end="",
            flush=True
        )

        current_leader = (
            self.election.get_leader()
        )

        if (
            failed_process_id is not None
            and not still_connected
            and failed_process_id == current_leader
            and self.running
        ):
            # The leader itself disconnected -> trigger a new election
            print(
                "\n[BULLY] The leader failed. "
                "Starting a new election."
            )

            self.election.handle_leader_failure()

        elif (
            self.running
            and self.is_leader()
            and not still_connected
        ):
            # We are the leader and lost a regular peer -> update everyone's peer list
            self.broadcast_peer_list()

        if self.running:
            self.update_heartbeat_roles()

    # ------------------------------------------------------------
    # Shut down the program
    # ------------------------------------------------------------

    def stop(self):
        # shuts down: stops heartbeat monitoring, closes the
        # listener socket, and closes every peer connection
        self.running = False
        self.heartbeat.stop()

        listener_socket = self.listener_socket

        if listener_socket is not None:
            try:
                listener_socket.close()

            except OSError:
                pass

        with self.connection_lock:
            connections = list(
                self.connections
            )

        for connection in connections:
            self.remove_connection(connection)

        print("[SHUTDOWN] Peer stopped.")