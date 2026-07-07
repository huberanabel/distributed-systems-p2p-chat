import threading

from discovery import Discovery
from peer import Peer


def main():
    username = input("Username: ").strip()
    # The port must always be entered explicitly
    while True:
        port_input = input("Your port: ").strip()

        try:
            port = int(port_input)

            if not (0 < port <= 65535):
                raise ValueError

            break

        except ValueError:
            print(
                "Please enter a valid port number "
                "(1-65535)."
            )

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
                    print(
                        f"[BULLY] Current leader: "
                        f"{peer.username} ({leader}) (this peer)"
                    )

                else:
                    leader_name = peer.get_username_for_process_id(
                        leader
                    )

                    if leader_name is None:
                        leader_name = "unknown"

                    print(
                        f"[BULLY] Current leader: "
                        f"{leader_name} ({leader})"
                    )

                continue

            peer.send_message(text)

    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Ctrl+C received.")

    finally:
        discovery.stop()
        peer.stop()
        print("[SHUTDOWN] Program finished.")


if __name__ == "__main__":
    main()