"""
Group View Management — tracks who is currently in the server cluster.

A "view" is a consistent snapshot of group membership.
When servers join or leave, the leader proposes a view change.
"""

import threading

from common.network import form_ring


class GroupView:
    """Manages group membership."""

    def __init__(self, my_ip, logger):
        self.my_ip = my_ip
        self.logger = logger
        self.view_id = 0
        self.members = []
        self.clients = {}  # client_key -> username
        self._lock = threading.Lock()

    def install_view(self, member_ips, reason=""):
        """Install a new view."""
        with self._lock:
            old = set(self.members)
            self.members = sorted(member_ips)
            self.view_id += 1
            new = set(self.members)

        joined = new - old
        left = old - new

        self.logger.view(
            f"View #{self.view_id} installed",
            f"Members: {self.members}\n"
            f"{'':36}Joined: {sorted(joined) if joined else 'none'}\n"
            f"{'':36}Left: {sorted(left) if left else 'none'}\n"
            f"{'':36}Reason: {reason}")

    def add_client(self, client_key, username):
        with self._lock:
            self.clients[client_key] = username
        self.logger.view(f"Client joined: {username} ({client_key})")

    def remove_client(self, client_key):
        with self._lock:
            username = self.clients.pop(client_key, client_key)
        self.logger.view(f"Client left: {username}")

    def get_members(self):
        with self._lock:
            return list(self.members)

    def get_view_id(self):
        with self._lock:
            return self.view_id
