import json
import socket
import threading
import time


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
        while self.running:
            self.send_announcement()
            time.sleep(ANNOUNCEMENT_INTERVAL)

    def send_announcement(self):
        message = {
            "type": "DISCOVERY",
            "username": self.peer.username,
            "host": self.peer.host,
            "port": self.peer.port,
            "peer_id": self.peer.peer_id,
            "process_id": self.peer.process_id
        }

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

        try:
            announcement_socket.sendto(
                json.dumps(message).encode("utf-8"),
                (BROADCAST_IP, DISCOVERY_PORT)
            )

        except OSError as error:
            if self.running:
                print(
                    f"[DISCOVERY ERROR] "
                    f"Could not send announcement: {error}"
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

                if message.get("type") != "DISCOVERY":
                    continue

                remote_peer_id = message.get("peer_id")

                if remote_peer_id == self.peer.peer_id:
                    continue

                remote_username = message["username"]
                remote_port = int(message["port"])
                remote_process_id = int(
                    message["process_id"]
                )

                # Use the real source IP of the UDP packet.
                remote_host = address[0]

                # Important for the Bully algorithm.
                self.peer.register_member(
                    remote_process_id
                )

                peer_address = (
                    remote_host,
                    remote_port
                )

                if peer_address in self.peer.known_peers:
                    continue

                # Only the peer with the smaller process ID creates
                # the TCP connection.
                #
                # This prevents:
                # Peer A -> Peer B
                # and at the same time
                # Peer B -> Peer A
                #
                # One TCP connection is enough because TCP is
                # bidirectional.
                if self.peer.process_id > remote_process_id:
                    continue

                print(
                    f"[DISCOVERY] Found peer "
                    f"{remote_username} at "
                    f"{remote_host}:{remote_port} "
                    f"with process ID "
                    f"{remote_process_id}"
                )

                self.peer.connect_to_peer(
                    remote_host,
                    remote_port
                )

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

    def stop(self):
        self.running = False

        if self.listener_socket is not None:
            try:
                self.listener_socket.close()
            except OSError:
                pass

        print("[DISCOVERY] Stopped.")