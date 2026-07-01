import json
import socket
import threading
import time
import uuid

from message import Message
from heartbeat import HeartbeatManager
from election import BullyElection


class Peer:

    def __init__(self, username, host, port):

        self.peer_id = str(uuid.uuid4())

        self.username = username
        self.host = host
        self.port = int(port)

        self.connections = []
        self.known_peers = set()

        # The port is used as the process ID for the Bully algorithm.
        self.process_id = self.port

        # Contains all currently known process IDs.
        self.member_ids = {self.process_id}

        self.running = True

        self.connection_lock = threading.Lock()

        # One receive buffer for every TCP connection.
        self.receive_buffers = {}

        # Stores information about connected peers.
        self.remote_peers = {}

        self.heartbeat = HeartbeatManager(
            send_packet=self.send_control_packet,
            on_timeout=self.handle_peer_timeout
        )

        self.heartbeat.start()

        self.election = BullyElection(
            process_id=self.process_id,
            get_members=self.get_member_ids,
            broadcast_packet=self.broadcast_control_packet
        )

    def start_listener(self):

        # Inspired by simpleserver.py.
        # Difference: TCP instead of UDP.

        listener_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        listener_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

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
                connection, address = listener_socket.accept()

                self.connections.append(connection)
                self.receive_buffers[connection] = ""

                self.heartbeat.register_connection(
                    connection
                )

                print(
                    f"[CONNECTED] New peer connected "
                    f"from {address}"
                )

                # Inspired by simplemultiserver.py.
                # Each connection is handled separately.

                thread = threading.Thread(
                    target=self.handle_connection,
                    args=(connection,),
                    daemon=True
                )

                thread.start()

                self.send_hello(connection)

            except OSError:
                break

    def connect_to_peer(self, peer_host, peer_port):

        peer_port = int(peer_port)
        peer_address = (peer_host, peer_port)

        if peer_address in self.known_peers:
            return

        if (
            peer_host == self.host
            and peer_port == self.port
        ):
            return

        try:
            peer_socket = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )

            peer_socket.settimeout(5)
            peer_socket.connect(peer_address)
            peer_socket.settimeout(None)

            self.connections.append(peer_socket)
            self.known_peers.add(peer_address)

            self.receive_buffers[peer_socket] = ""

            self.heartbeat.register_connection(
                peer_socket
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

        except Exception as error:

            self.known_peers.discard(
                peer_address
            )

            print(
                f"[ERROR] Could not connect to peer "
                f"{peer_host}:{peer_port}: {error}"
            )

    def send_hello(self, connection):

        packet = {
            "type": "HELLO",
            "peer_id": self.peer_id,
            "username": self.username,
            "host": self.host,
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

            self.remove_connection(
                connection
            )

    def handle_connection(self, connection):

        while self.running:

            try:
                data = connection.recv(4096)

                if not data:
                    break

                self.heartbeat.mark_alive(
                    connection
                )

                self.receive_buffers[connection] += (
                    data.decode("utf-8")
                )

                while "\n" in self.receive_buffers[connection]:

                    packet_text, remaining_data = (
                        self.receive_buffers[connection].split(
                            "\n",
                            1
                        )
                    )

                    self.receive_buffers[connection] = (
                        remaining_data
                    )

                    if not packet_text.strip():
                        continue

                    packet = json.loads(
                        packet_text
                    )

                    packet_type = packet.get(
                        "type"
                    )

                    if packet_type == "HELLO":

                        self.handle_hello(
                            connection,
                            packet
                        )

                    elif packet_type == "HEARTBEAT":

                        continue

                    elif packet_type in {
                        "ELECTION",
                        "OK",
                        "COORDINATOR"
                    }:

                        self.election.handle_packet(
                            packet
                        )

                    elif packet_type == "CHAT_MESSAGE":

                        message = Message.from_json(
                            json.dumps(
                                packet["message"]
                            )
                        )

                        print(
                            f"\n[{message.sender_name}]: "
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

            except Exception as error:

                if self.running:
                    print(
                        f"[ERROR] Connection closed: "
                        f"{error}"
                    )

                break

        self.remove_connection(
            connection
        )

    def handle_hello(self, connection, packet):

        process_id = int(
            packet["process_id"]
        )

        peer_information = {
            "peer_id": packet["peer_id"],
            "username": packet["username"],
            "host": packet["host"],
            "port": int(packet["port"]),
            "process_id": process_id
        }

        self.remote_peers[connection] = (
            peer_information
        )

        self.member_ids.add(
            process_id
        )

        # Do not add packet["host"] to known_peers here.
        # It may be 0.0.0.0, which is only a bind address.

        print(
            f"[PEER READY] "
            f"{peer_information['username']} "
            f"uses process ID {process_id}"
        )

    def send_message(self, text):

        message = Message(
            sender_id=self.peer_id,
            sender_name=self.username,
            text=text,
            timestamp=time.time()
        )

        packet = {
            "type": "CHAT_MESSAGE",
            "message": json.loads(
                message.to_json()
            )
        }

        encoded_message = (
            json.dumps(packet) + "\n"
        ).encode("utf-8")

        for connection in list(
            self.connections
        ):

            try:
                connection.sendall(
                    encoded_message
                )

            except Exception as error:
                print(
                    f"[ERROR] Could not send message: "
                    f"{error}"
                )

                self.remove_connection(
                    connection
                )

    def send_control_packet(
        self,
        connection,
        packet
    ):

        encoded_packet = (
            json.dumps(packet) + "\n"
        ).encode("utf-8")

        connection.sendall(
            encoded_packet
        )

    def broadcast_control_packet(
        self,
        packet
    ):

        for connection in list(
            self.connections
        ):

            try:
                self.send_control_packet(
                    connection,
                    packet
                )

            except Exception as error:
                print(
                    f"[ERROR] Could not send control packet: "
                    f"{error}"
                )

                self.remove_connection(
                    connection
                )

    def register_member(
        self,
        process_id
    ):

        self.member_ids.add(
            int(process_id)
        )

    def get_member_ids(self):

        return set(
            self.member_ids
        )

    def start_election(self):

        self.election.start_election()

    def get_leader(self):

        return self.election.get_leader()

    def handle_peer_timeout(
        self,
        connection
    ):

        if connection not in self.connections:
            return

        print(
            "\n[FAULT TOLERANCE] "
            "A peer is no longer reachable."
        )

        self.remove_connection(
            connection
        )

    def remove_connection(
        self,
        connection
    ):

        if connection not in self.connections:
            return

        remote_peer = self.remote_peers.get(
            connection
        )

        failed_process_id = None

        if remote_peer is not None:
            failed_process_id = remote_peer[
                "process_id"
            ]

        current_leader = (
            self.election.get_leader()
        )

        self.connections.remove(
            connection
        )

        self.receive_buffers.pop(
            connection,
            None
        )

        self.remote_peers.pop(
            connection,
            None
        )

        self.heartbeat.unregister_connection(
            connection
        )

        if failed_process_id is not None:
            self.member_ids.discard(
                failed_process_id
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
            "\n[DISCONNECTED] "
            "A peer disconnected."
        )

        print(
            "> ",
            end="",
            flush=True
        )

        if (
            failed_process_id is not None
            and failed_process_id == current_leader
            and self.running
        ):

            print(
                "\n[BULLY] The leader failed. "
                "Starting a new election."
            )

            self.election.handle_leader_failure()

    def stop(self):

        self.running = False

        self.heartbeat.stop()

        for connection in list(
            self.connections
        ):

            self.remove_connection(
                connection
            )

        self.connections.clear()

        print(
            "[SHUTDOWN] Peer stopped."
        )