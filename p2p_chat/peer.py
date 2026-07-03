import json
import socket
import threading
import time
import uuid

from message import Message
from heartbeat import HeartbeatManager
from election import BullyElection
from lamport_clock import LamportClock


class Peer:

    def __init__(self, username, host, port):

        self.peer_id = str(uuid.uuid4())

        self.username = username
        self.host = host
        self.port = int(port)

        self.connections = []
        self.known_peers = set()

        self.process_id = self.port
        self.lamport_clock = LamportClock()

        self.member_ids = {self.process_id}

        self.running = True

        self.connection_lock = threading.Lock()

        self.receive_buffers = {}

        self.remote_peers = {}

        self.heartbeat = HeartbeatManager(
            send_packet=self.send_control_packet,
            on_timeout=self.handle_peer_timeout
        )

        self.heartbeat.start()

        self.election = BullyElection(
            process_id=self.process_id,
            get_members=self.get_member_ids,
            broadcast_packet=self.broadcast_control_packet,
            on_leader_change=self.on_leader_change
        )


    # ------------------------------------------------------------
    # Listening / connecting
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
                    connection,
                    active=False
                )

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
                peer_socket,
                active=False
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

    # ------------------------------------------------------------
    # Receiving
    # ------------------------------------------------------------

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

                    elif packet_type == "PEER_LIST":

                        self.handle_peer_list(
                            packet
                        )

                    elif packet_type == "HEARTBEAT":

                        try:
                            self.send_control_packet(
                                connection,
                                {"type": "HEARTBEAT_ACK"}
                            )
                        except Exception:
                            pass

                    elif packet_type == "HEARTBEAT_ACK":

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

                        self.lamport_clock.receive_event(
                            message.lamport_time
                        )

                        print(
                            f"\n[L={self.lamport_clock.get_time()}] "
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

        existing_connection = self.get_connection_for_process_id(
            process_id
        )

        if (
            existing_connection is not None
            and existing_connection is not connection
        ):
            print(
                f"[PEER] Duplicate connection to process "
                f"{process_id} detected. Closing the redundant "
                f"connection."
            )

            self.remove_connection(connection)
            return

        try:
            real_host = connection.getpeername()[0]
        except OSError:
            real_host = None

        peer_information = {
            "peer_id": packet["peer_id"],
            "username": packet["username"],
            "host": real_host,
            "port": int(packet["port"]),
            "process_id": process_id
        }

        self.remote_peers[connection] = (
            peer_information
        )

        self.member_ids.add(
            process_id
        )

        print(
            f"[PEER READY] "
            f"{peer_information['username']} "
            f"uses process ID {process_id}"
        )

        if self.is_leader():
            self.broadcast_peer_list()

        self.update_heartbeat_roles()

    def handle_peer_list(self, packet):
        """
        Handles a PEER_LIST message sent by the leader.

        Connects to any peer contained in the list that we are not
        already connected to, which forms the full/partial mesh
        described in the specification. Also adopts the sender as
        the current leader if we did not already know one.
        """

        try:
            leader_process_id = int(packet["leader_process_id"])
            entries = packet.get("peers", [])
        except (KeyError, TypeError, ValueError):
            print("[PEER_LIST ERROR] Invalid PEER_LIST packet.")
            return

        if self.election.get_leader() != leader_process_id:
            self.election.set_leader(leader_process_id)

        for entry in entries:
            try:
                remote_process_id = int(entry["process_id"])
                remote_host = entry.get("host")
                remote_port = int(entry["port"])
            except (KeyError, TypeError, ValueError):
                continue

            if remote_process_id == self.process_id:
                continue

            self.register_member(remote_process_id)

            if remote_process_id == leader_process_id:
                continue

            if self.get_connection_for_process_id(remote_process_id) is not None:
                continue

            if not remote_host or remote_host == "0.0.0.0":
                continue

            if self.process_id > remote_process_id:
                continue

            self.connect_to_peer(remote_host, remote_port)

        self.update_heartbeat_roles()

    # ------------------------------------------------------------
    # Sending
    # ------------------------------------------------------------

    def send_message(self, text):

        lamport_time = self.lamport_clock.send_event()

        message = Message(
            sender_id=self.peer_id,
            sender_name=self.username,
            text=text,
            lamport_time=lamport_time
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

    def broadcast_peer_list(self):

        entries = [
            {
                "peer_id": self.peer_id,
                "username": self.username,
                "host": self.host,
                "port": self.port,
                "process_id": self.process_id
            }
        ]

        for information in self.remote_peers.values():
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
    # Membership / election helpers
    # ------------------------------------------------------------

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

    def get_connection_for_process_id(self, process_id):

        for connection, information in self.remote_peers.items():
            if information["process_id"] == process_id:
                return connection

        return None

    def get_username_for_process_id(self, process_id):

        if process_id == self.process_id:
            return self.username

        for information in self.remote_peers.values():
            if information["process_id"] == process_id:
                return information["username"]

        return None

    def start_election(self):

        self.election.start_election()

    def get_leader(self):

        return self.election.get_leader()

    def is_leader(self):

        leader_id = self.election.get_leader()
        return leader_id is not None and leader_id == self.process_id

    def on_leader_change(self, leader_id):

        self.update_heartbeat_roles()

        if self.is_leader():
            self.broadcast_peer_list()

    def update_heartbeat_roles(self):

        leader_id = self.get_leader()

        if leader_id is None:
            return

        if self.is_leader():
            for connection in list(self.connections):
                self.heartbeat.set_active(connection, True)
            return

        leader_connection = self.get_connection_for_process_id(leader_id)

        for connection in list(self.connections):
            if connection is leader_connection:
                self.heartbeat.set_active(connection, False)
            else:
                self.heartbeat.unregister_connection(connection)

    # ------------------------------------------------------------
    # Failure handling
    # ------------------------------------------------------------

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
        failed_username = "Unknown peer"

        if remote_peer is not None:
            failed_process_id = remote_peer[
                "process_id"
            ]
            failed_username = remote_peer[
                "username"
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
            f"\n[DISCONNECTED] "
            f"{failed_username} disconnected."
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

        elif self.running and self.is_leader():

            self.broadcast_peer_list()

        self.update_heartbeat_roles()

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