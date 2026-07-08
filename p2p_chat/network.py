import socket


def get_local_ip():
    # Opens a UDP "connection" to a public IP just to let the OS pick
    # which local network interface/IP it would use for outgoing
    # traffic. No actual packets are sent (UDP connect is local-only).
    probe_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        probe_socket.connect(("8.8.8.8", 80))
        return probe_socket.getsockname()[0]

    except OSError:
        # Fallback if there is no network route at all.
        return "127.0.0.1"

    finally:
        probe_socket.close()


def get_subnet_broadcast_address(local_ip):
    # Naively assumes a /24 subnet and builds the directed broadcast
    # address (e.g. 192.168.1.23 -> 192.168.1.255). Used as a more
    # reliable alternative to 255.255.255.255 on some adapters.
    parts = local_ip.split(".")

    if len(parts) != 4:
        return None

    return ".".join(parts[:3] + ["255"])