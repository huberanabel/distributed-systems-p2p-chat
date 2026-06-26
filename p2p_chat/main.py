import threading
from discovery import Discovery

from peer import Peer


def main():
    username = input("Username: ")
    port = int(input("Your port: "))

    peer = Peer(
        username=username,
        host="127.0.0.1",
        port=port
    )

    listener_thread = threading.Thread(

        target=peer.start_listener,

        daemon=True

    )

    listener_thread.start()

    discovery = Discovery(peer)

    discovery.start()

    #connect = input("Connect to another peer? y/n: ")

    #if connect.lower() == "y":
        #peer_host = input("Peer host: ")
        #peer_port = int(input("Peer port: "))
        #peer.connect_to_peer(peer_host, peer_port)

    print("\nYou can now write messages.")
    print("Type 'exit' to stop.\n")

    while True:
        text = input()

        if text.lower() == "exit":
            break

        peer.send_message(text)


if __name__ == "__main__":
    main()