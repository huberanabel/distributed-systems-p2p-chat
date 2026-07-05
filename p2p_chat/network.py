import socket


def get_local_ip():

    probe_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        probe_socket.connect(("8.8.8.8", 80))
        return probe_socket.getsockname()[0]

    except OSError:
        return "127.0.0.1"

    finally:
        probe_socket.close()


def get_subnet_broadcast_address(local_ip):

    parts = local_ip.split(".")

    if len(parts) != 4:
        return None

    return ".".join(parts[:3] + ["255"])