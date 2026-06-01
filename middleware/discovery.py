"""
Dynamic Discovery Service using UDP broadcast.
Based on broadcastsender.py and broadcastlistener.py example code.

Mechanism:
- Servers send broadcast announcements on the subnet broadcast address.
- All nodes listen for these broadcasts.
- When a new node is heard, it is added to the known members list.
"""

import socket
import json
import threading
import time

from common.constants import (
    BROADCAST_IP, BROADCAST_PORT, DISCOVERY_INTERVAL,
    MSG_DISCOVERY_ANNOUNCE, MSG_DISCOVERY_RESPONSE,
    ROLE_SERVER, ROLE_CLIENT, BUFFER_SIZE,
)
from common.network import get_local_ip


class DiscoveryService:
    """UDP broadcast-based discovery for servers and clients."""

    def __init__(self, node_ip, role, logger):
        self.node_ip = node_ip
        self.role = role
        self.logger = logger
        self.discovered_nodes = {}  # ip -> {role, last_seen}
        self._lock = threading.Lock()
        self._running = False

    def start(self):
        self._running = True
        if self.role == ROLE_SERVER:
            t = threading.Thread(target=self._broadcast_loop, daemon=True)
            t.start()

        t = threading.Thread(target=self._listen_loop, daemon=True)
        t.start()
        self.logger.discovery("Discovery service started",
                              f"Role={self.role}, Broadcast={BROADCAST_IP}:{BROADCAST_PORT}")

    def stop(self):
        self._running = False

    def get_servers(self):
        """Return dict of discovered server IPs."""
        with self._lock:
            return {ip: info for ip, info in self.discovered_nodes.items()
                    if info["role"] == ROLE_SERVER}

    def get_alive_servers(self, max_age=None):
        """Return servers seen within max_age seconds (default: 2x discovery interval)."""
        if max_age is None:
            max_age = DISCOVERY_INTERVAL * 3
        now = time.time()
        with self._lock:
            return {ip: info for ip, info in self.discovered_nodes.items()
                    if info["role"] == ROLE_SERVER
                    and now - info["last_seen"] < max_age}

    def get_all_nodes(self):
        with self._lock:
            return dict(self.discovered_nodes)

    def remove_node(self, ip):
        """Remove a known-dead node from the discovery cache."""
        with self._lock:
            self.discovered_nodes.pop(ip, None)

    def _broadcast_loop(self):
        """Periodically broadcast this server's presence (like broadcastsender.py)."""
        while self._running:
            try:
                broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

                announcement = json.dumps({
                    "type": MSG_DISCOVERY_ANNOUNCE,
                    "ip": self.node_ip,
                    "role": self.role,
                })

                broadcast_socket.sendto(
                    str.encode(announcement), (BROADCAST_IP, BROADCAST_PORT))
                broadcast_socket.close()

                self.logger.discovery(
                    f"Broadcast announcement sent",
                    f"IP={self.node_ip}, Port={BROADCAST_PORT}")
            except Exception as e:
                self.logger.fault(f"Broadcast send error: {e}")

            time.sleep(DISCOVERY_INTERVAL)

    def _listen_loop(self):
        """Listen for broadcast announcements (like broadcastlistener.py)."""
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        listen_socket.bind(("0.0.0.0", BROADCAST_PORT))
        listen_socket.settimeout(1.0)

        while self._running:
            try:
                data, addr = listen_socket.recvfrom(BUFFER_SIZE)
                if data:
                    msg = json.loads(data.decode())
                    sender_ip = msg.get("ip", addr[0])

                    if sender_ip == self.node_ip:
                        continue

                    with self._lock:
                        is_new = sender_ip not in self.discovered_nodes
                        self.discovered_nodes[sender_ip] = {
                            "role": msg.get("role", ROLE_SERVER),
                            "last_seen": time.time(),
                        }

                    if is_new:
                        self.logger.discovery(
                            f"Discovered new node: {sender_ip}",
                            f"Role={msg.get('role')}")

            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    self.logger.fault(f"Broadcast listen error: {e}")

        listen_socket.close()
