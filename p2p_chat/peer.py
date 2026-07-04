import json
import socket
import threading
import uuid

from message import Message
from heartbeat import HeartbeatManager
from election import BullyElection
from lamport_clock import LamportClock


MAX_PACKET_SIZE = 1_048_576
MAX_RECEIVE_BUFFER = MAX_PACKET_SIZE


class Peer:

    def __init__(self, username, host, port):
        self.peer_id = str(uuid.uuid4())
        self.username = username
        self.host = host
        self.port = int(port)

        # Eindeutige Prozess-ID, unabhängig vom verwendeten Port.
        self.process_id = uuid.uuid4().int & ((1 << 63) - 1)

        if self.process_id == 0:
            self.process_id = 1

        self.connections = []
        self.known_peers = set()
        self.member_ids = {self.process_id}

        self.running = True

        # Schützt gemeinsame Daten vor parallelen Thread-Zugriffen.
        self.connection_lock = threading.RLock()

        # Wird gespeichert, damit stop() den Listener schließen kann.
        self.listener_socket = None

        # Empfangsdaten werden als Bytes gespeichert.
        self.receive_buffers = {}

        self.remote_peers = {}
        self.connection_addresses = {}
        self.outgoing_connections = set()

        # Verhindert, dass mehrere Threads gleichzeitig auf
        # denselben Socket schreiben.
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
            on_leader_change=self.on_leader_change
        )

        self.heartbeat.start()

    # ------------------------------------------------------------
    # Verbindungen registrieren
    # ------------------------------------------------------------

    def _register_connection(
        self,
        connection,
        peer_address=None,
        outgoing=False
    ):
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

            print(f"[PEER ID] {self.peer_id}")
            print(f"[PROCESS ID] {self.process_id}")

            while self.running:
                try:
                    connection, address = (
                        listener_socket.accept()
                    )

                except socket.timeout:
                    continue

                except OSError:
                    break

                self._register_connection(connection)

                print(
                    f"[CONNECTED] New peer connected "
                    f"from {address}"
                )

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
    # Verbindung zu Peer
    # ------------------------------------------------------------

    def connect_to_peer(self, peer_host, peer_port):
        peer_port = int(peer_port)
        peer_address = (peer_host, peer_port)

        if (
            peer_port == self.port
            and peer_host in {
                self.host,
                "127.0.0.1",
                "localhost",
                "0.0.0.0"
            }
        ):
            return

        with self.connection_lock:
            if peer_address in self.known_peers:
                return

            # Adresse vorübergehend reservieren.
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
                f"[CONNECTED] Connected to peer "
                f"{peer_host}:{peer_port}"
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
    # HELLO senden
    # ------------------------------------------------------------

    def send_hello(self, connection):
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
    # Daten empfangen
    # ------------------------------------------------------------

    def handle_connection(self, connection):
        while self.running:
            try:
                data = connection.recv(4096)

                if not data:
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
                        raise ValueError(
                            "Receive buffer is too large."
                        )

                    packet_lines = []

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
    # Einzelnes Paket verarbeiten
    # ------------------------------------------------------------

    def _handle_packet(self, connection, packet):
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
            self.send_control_packet(
                connection,
                {
                    "type": "HEARTBEAT_ACK"
                }
            )

        elif packet_type == "HEARTBEAT_ACK":
            return

        elif packet_type in {
            "ELECTION",
            "OK",
            "COORDINATOR"
        }:
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

            if message.sender_id != remote_peer["peer_id"]:
                raise ValueError(
                    "Chat message has a false sender_id."
                )

            if message.sender_name != remote_peer["username"]:
                raise ValueError(
                    "Chat message has a false sender_name."
                )

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
    # Peer-Information abrufen
    # ------------------------------------------------------------

    def _get_remote_peer(self, connection):
        with self.connection_lock:
            information = self.remote_peers.get(
                connection
            )

            if information is None:
                return None

            return dict(information)

    # ------------------------------------------------------------
    # Doppelte Verbindung vergleichen
    # ------------------------------------------------------------

    def _connection_rank(self, connection):
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
    # HELLO verarbeiten
    # ------------------------------------------------------------

    def handle_hello(self, connection, packet):
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
                f"[PEER] Duplicate connection to process "
                f"{process_id} detected. "
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

            if (
                connection
                not in self.connection_addresses
                and real_host is not None
            ):
                peer_address = (
                    real_host,
                    peer_port
                )

                self.connection_addresses[
                    connection
                ] = peer_address

                self.known_peers.add(peer_address)

        print(
            f"[PEER READY] "
            f"{peer_information['username']} "
            f"uses process ID {process_id}"
        )

        if (
            self.is_leader()
            and process_id > self.process_id
        ):
            self.start_election()

        elif self.is_leader():
            self.broadcast_peer_list()

        self.update_heartbeat_roles()

    # ------------------------------------------------------------
    # Peer-Liste verarbeiten
    # ------------------------------------------------------------

    def handle_peer_list(self, connection, packet):
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
            print(
                "[PEER_LIST ERROR] The peer list "
                "was not sent by its claimed leader."
            )

            return

        if leader_process_id < self.process_id:
            print(
                "[PEER_LIST ERROR] A lower process "
                "cannot be leader. "
                "Starting a new election."
            )

            self.start_election()
            return

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
                continue

            if (
                self.get_connection_for_process_id(
                    remote_process_id
                )
                is not None
            ):
                continue

            if (
                not remote_host
                or remote_host == "0.0.0.0"
            ):
                continue

            if self.process_id > remote_process_id:
                continue

            self.connect_to_peer(
                remote_host,
                remote_port
            )

        self.update_heartbeat_roles()

    # ------------------------------------------------------------
    # Chatnachricht senden
    # ------------------------------------------------------------

    def send_message(self, text):
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
    # Kontrollpaket senden
    # ------------------------------------------------------------

    def send_control_packet(
        self,
        connection,
        packet
    ):
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

        with send_lock:
            connection.sendall(encoded_packet)

    # ------------------------------------------------------------
    # Paket an alle Peers senden
    # ------------------------------------------------------------

    def broadcast_control_packet(self, packet):
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
    # Peer-Liste senden
    # ------------------------------------------------------------

    def broadcast_peer_list(self):
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
                "host": self.host,
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
    # Mitgliederverwaltung
    # ------------------------------------------------------------

    def register_member(self, process_id):
        with self.connection_lock:
            self.member_ids.add(
                int(process_id)
            )

    def get_member_ids(self):
        with self.connection_lock:
            return set(self.member_ids)

    def get_connection_for_process_id(
        self,
        process_id
    ):
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

        return None

    # ------------------------------------------------------------
    # Election-Hilfsmethoden
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
        self.update_heartbeat_roles()

        if self.is_leader():
            self.broadcast_peer_list()

    # ------------------------------------------------------------
    # Heartbeat-Rollen aktualisieren
    # ------------------------------------------------------------

    def update_heartbeat_roles(self):
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
    # Timeout behandeln
    # ------------------------------------------------------------

    def handle_peer_timeout(self, connection):
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
    # Verbindung entfernen
    # ------------------------------------------------------------

    def remove_connection(self, connection):
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
            self.broadcast_peer_list()

        if self.running:
            self.update_heartbeat_roles()

    # ------------------------------------------------------------
    # Programm beenden
    # ------------------------------------------------------------

    def stop(self):
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