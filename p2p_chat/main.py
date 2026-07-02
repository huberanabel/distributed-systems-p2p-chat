import threading

from discovery import Discovery
from peer import Peer


def main():
    username = input("Username: ")
    port = int(input("Your port: "))

    peer = Peer(
        username=username,
        host="0.0.0.0",
        port=port
    )

    listener_thread = threading.Thread(
        target=peer.start_listener,
        daemon=True
    )

    listener_thread.start()

    discovery = Discovery(peer)
    discovery.start()

    print("\nYou can now write messages.")
    print("Commands:")
    print("  /leader -> show current leader")
    print("  exit    -> stop the peer\n")

    try:
        while True:
            text = input("> ")

            if text.lower() == "exit":
                break

            if text.lower() == "/leader":
                leader = peer.get_leader()

                if leader is None:
                    print("[BULLY] No leader has been elected.")
                elif leader == peer.process_id:
                    print(f"[BULLY] Current leader: {leader} (this peer)")
                else:
                    print(f"[BULLY] Current leader: {leader}")

                continue

            peer.send_message(text)

    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Ctrl+C received.")

    finally:
        discovery.stop()

        if hasattr(peer, "stop"):
            peer.stop()

        print("[SHUTDOWN] Program finished.")


if __name__ == "__main__":
    main()