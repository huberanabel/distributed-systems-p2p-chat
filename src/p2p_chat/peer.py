import socket

import threading

import time

import uuid

from message import Message


class Peer:

    def __init__(self, username, host, port):

        self.peer_id = str(uuid.uuid4())

        self.username = username

        self.host = host

        self.port = port

        self.connections = []

    def start_listener(self):

        # Inspired by simpleserver.py

        # Difference: TCP instead of UDP

        listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        listener_socket.bind((self.host, self.port))

        listener_socket.listen()

        print(f"[STARTED] Peer {self.username} is running on {self.host}:{self.port}")

        print(f"[PEER ID] {self.peer_id}")

        while True:
            connection, address = listener_socket.accept()

            self.connections.append(connection)

            print(f"[CONNECTED] New peer connected from {address}")

            # Inspired by simplemultiserver.py

            # Each connection is handled separately

            thread = threading.Thread(

                target=self.handle_connection,

                args=(connection,),

                daemon=True

            )

            thread.start()

    def connect_to_peer(self, peer_host, peer_port):

        # Inspired by simpleclient.py

        # Difference: TCP connect instead of UDP sendto

        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        peer_socket.connect((peer_host, peer_port))

        self.connections.append(peer_socket)

        print(f"[CONNECTED] Connected to peer {peer_host}:{peer_port}")

        thread = threading.Thread(

            target=self.handle_connection,

            args=(peer_socket,),

            daemon=True

        )

        thread.start()

    def handle_connection(self, connection):

        while True:

            try:

                data = connection.recv(4096)

                if not data:
                    break

                message = Message.from_json(data.decode("utf-8"))

                print(f"\n[{message.sender_name}]: {message.text}")

            except Exception as error:

                print(f"[ERROR] Connection closed: {error}")

                break

        if connection in self.connections:
            self.connections.remove(connection)

        connection.close()

    def send_message(self, text):

        message = Message(

            sender_id=self.peer_id,

            sender_name=self.username,

            text=text,

            timestamp=time.time()

        )

        encoded_message = message.to_json().encode("utf-8")

        for connection in self.connections:

            try:

                connection.sendall(encoded_message)

            except Exception as error:

                print(f"[ERROR] Could not send message: {error}")
