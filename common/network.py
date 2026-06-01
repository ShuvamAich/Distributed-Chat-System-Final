"""
Network utility functions — get local IP, form ring, get neighbour.
Based on ring.py example code pattern.
"""

import socket


def get_local_ip():
    """Get the local IP address used for LAN communication."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def form_ring(members):
    """
    Form a ring from a list of member IPs.
    Sort by binary IP representation (same as ring.py example).
    Returns sorted list of IP strings.
    """
    sorted_binary_ring = sorted([socket.inet_aton(member) for member in members])
    sorted_ip_ring = [socket.inet_ntoa(node) for node in sorted_binary_ring]
    return sorted_ip_ring


def get_neighbour(ring, current_node_ip, direction='left'):
    """
    Get the neighbour in the ring.
    'left' = next node clockwise (index + 1, wrapping).
    Based on ring.py example code.
    """
    current_node_index = ring.index(current_node_ip) if current_node_ip in ring else -1
    if current_node_index != -1:
        if direction == 'left':
            if current_node_index + 1 == len(ring):
                return ring[0]
            else:
                return ring[current_node_index + 1]
        else:
            if current_node_index == 0:
                return ring[len(ring) - 1]
            else:
                return ring[current_node_index - 1]
    else:
        return None
