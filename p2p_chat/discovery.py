import json
import socket
import threading
import time

from network import get_local_ip, get_subnet_broadcast_address


DISCOVERY_PORT = 5973
BROADCAST_IP = "255.255.255.255"
ANNOUNCEMENT_INTERVAL = 5


class Discovery:

    def __init__(self, peer):
        self.peer = peer
        self.running = True
        self.listener_socket = None

    def start(self):
        listener_thread = threading.Thread(
            target=self.listen,
            daemon=True
        )
        listener_thread.start()

        announcement_thread = threading.Thread(
            target=self.announce_loop,
            daemon=True
        )
        announcement_thread.start()

    def announce_loop(self):
        first_attempt = True

        while self.running:

            if self.peer.get_leader() is not None:
                leader_username = self.peer.get_username_for_process_id(
                    self.peer.get_leader()
                )

                if leader_username is None:
                    leader_username = "Unknown"

                print(
                    f"[DISCOVERY] Leader is known: "
                    f"{leader_username}. "
                    f"Stopping discovery announcements."
                )
                return

            self.send_announcement()
            time.sleep(ANNOUNCEMENT_INTERVAL)

            if first_attempt and self.peer.get_leader() is None:
                first_attempt = False
                self.peer.start_election()

    def send_announcement(self):
        message = {
            "type": "DISCOVERY_REQUEST",
            "peer_id": self.peer.peer_id,
            "username": self.peer.username,
            "process_id": self.peer.process_id
        }

        self._send_broadcast(message)

    def _send_broadcast(self, message):
        announcement_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )

        announcement_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_BROADCAST,
            1
        )

        announcement_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        encoded_message = json.dumps(message).encode("utf-8")

        # Some network adapters (notably virtual/host-only adapters
        # such as VirtualBox's) do not reliably forward the generic
        # "limited broadcast" address 255.255.255.255. Sending to
        # both the limited broadcast AND a best-effort, subnet-
        # directed broadcast address (e.g. 192.168.56.255) covers
        # both cases without needing extra dependencies to read the
        # real interface netmask.
        target_addresses = {BROADCAST_IP}

        local_ip = get_local_ip()
        subnet_broadcast = get_subnet_broadcast_address(local_ip)

        if subnet_broadcast is not None:
            target_addresses.add(subnet_broadcast)

        try:
            for target_ip in target_addresses:
                try:
                    announcement_socket.sendto(
                        encoded_message,
                        (target_ip, DISCOVERY_PORT)
                    )

                except OSError as error:
                    if self.running:
                        print(
                            f"[DISCOVERY ERROR] "
                            f"Could not send broadcast to "
                            f"{target_ip}: {error}"
                        )

        finally:
            announcement_socket.close()

    def listen(self):
        listener_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )

        listener_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        try:
            listener_socket.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_REUSEPORT,
                1
            )

        except (AttributeError, OSError):
            pass

        listener_socket.bind(
            ("", DISCOVERY_PORT)
        )

        listener_socket.settimeout(1.0)

        self.listener_socket = listener_socket

        print("[DISCOVERY] Listening for peers...")

        while self.running:
            try:
                data, address = listener_socket.recvfrom(4096)

            except socket.timeout:
                continue

            except OSError:
                break

            try:
                message = json.loads(
                    data.decode("utf-8")
                )

                message_type = message.get("type")

                if message_type == "DISCOVERY_REQUEST":
                    self._handle_discovery_request(message)

                elif message_type == "DISCOVERY_RESPONSE":
                    self._handle_discovery_response(message, address)


            except (
                json.JSONDecodeError,
                KeyError,
                TypeError,
                ValueError
            ) as error:
                print(
                    f"[DISCOVERY ERROR] "
                    f"Invalid discovery message: {error}"
                )

    def _handle_discovery_request(self, message):
        remote_peer_id = message.get("peer_id")

        if remote_peer_id == self.peer.peer_id:
            return

        remote_process_id = int(message["process_id"])

        self.peer.register_member(remote_process_id)

        if not self.peer.is_leader():
            return

        response = {
            "type": "DISCOVERY_RESPONSE",
            "target_peer_id": remote_peer_id,
            "leader_port": self.peer.port,
            "leader_process_id": self.peer.process_id
        }

        self._send_broadcast(response)

    def _handle_discovery_response(self, message, address):
        target_peer_id = message.get("target_peer_id")

        if target_peer_id != self.peer.peer_id:
            return

        leader_process_id = int(message["leader_process_id"])
        leader_port = int(message["leader_port"])

        if leader_process_id == self.peer.process_id:
            return

        if self.peer.get_connection_for_process_id(leader_process_id) is not None:
            return

        leader_host = address[0]

        print(
            f"[DISCOVERY] Leader found: process "
            f"{leader_process_id} at {leader_host}:{leader_port}"
        )

        self.peer.register_member(leader_process_id)
        self.peer.connect_to_peer(leader_host, leader_port)

    def stop(self):
        self.running = False

        if self.listener_socket is not None:
            try:
                self.listener_socket.close()
            except OSError:
                pass

        print("[DISCOVERY] Stopped.")