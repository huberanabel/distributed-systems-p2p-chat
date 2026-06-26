import socket
import threading
import json
import time

DISCOVERY_PORT = 5973
BROADCAST_IP = "255.255.255.255"


class Discovery:
    def __init__(self, peer):
        self.peer = peer
        self.running = True

    def start(self):
        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.announce_loop, daemon=True).start()

    def announce_loop(self):
        while self.running:
            self.send_announcement()
            time.sleep(5)

    def send_announcement(self):
        message = {
            "type": "DISCOVERY",
            "username": self.peer.username,
            "host": self.peer.host,
            "port": self.peer.port,
            "peer_id": self.peer.peer_id
        }

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.sendto(json.dumps(message).encode(), (BROADCAST_IP, DISCOVERY_PORT))
        sock.close()

    def listen(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass

        sock.bind(("", DISCOVERY_PORT))

        print("[DISCOVERY] Listening for peers...")

        while self.running:
            data, addr = sock.recvfrom(1024)

            try:
                message = json.loads(data.decode())

                if message.get("type") != "DISCOVERY":
                    continue

                if message.get("peer_id") == self.peer.peer_id:
                    continue

                host = message["host"]
                port = int(message["port"])
                username = message["username"]

                if self.peer.port > port:
                    continue

                peer_address = (host, port)

                if peer_address in self.peer.known_peers:
                    continue

                print(f"[DISCOVERY] Found peer {username} at {host}:{port}")

                self.peer.connect_to_peer(host, port)

            except Exception as e:
                print("[DISCOVERY ERROR]", e)