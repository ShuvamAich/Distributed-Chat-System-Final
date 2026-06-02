"""
Leader Election using the LCR (LeLann-Chang-Roberts) Ring Algorithm.
Based on lcr-template.py and ring.py example code.

Uses UDP sockets for ring communication (just like the example).
Node IDs are IP addresses, sorted by binary IP to form the ring.

How it works:
1. Servers form a ring sorted by IP (using ring.py's form_ring).
2. Each node sends its IP (as 'mid') to its left neighbour via UDP.
3. A node forwards a received 'mid' only if it's greater than its own.
4. When a node receives its own 'mid' back, it is the leader.
5. The leader sends an 'isLeader: True' message around the ring.
"""

import socket
import json
import threading
import time

from common.constants import RING_PORT, BUFFER_SIZE, ELECTION_TIMEOUT
from common.network import form_ring, get_neighbour


class LCRElection:
    """LCR Ring-based leader election using UDP (based on lcr-template.py)."""

    def __init__(self, my_ip, logger, get_alive_callback=None):
        self.my_ip = my_ip
        self.logger = logger
        self._get_alive_callback = get_alive_callback
        self.ring = []
        self.neighbour = None  # left neighbour (IP, port) tuple
        self.leader_ip = None
        self.participant = False
        self._lock = threading.Lock()
        self._running = False
        self._election_event = threading.Event()
        self._ring_socket = None

    def start(self):
        """Start the ring listener."""
        self._running = True
        self._ring_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._ring_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._ring_socket.bind(("0.0.0.0", RING_PORT))
        self._ring_socket.settimeout(1.0)

        t = threading.Thread(target=self._listen_loop, daemon=True)
        t.start()
        self.logger.election(f"Ring listener started on {self.my_ip}:{RING_PORT}")

    def stop(self):
        self._running = False
        if self._ring_socket:
            self._ring_socket.close()

    def update_ring(self, member_ips):
        """Update the ring using form_ring (binary IP sort from ring.py)."""
        with self._lock:
            self.ring = form_ring(member_ips)
            if len(self.ring) <= 1:
                self.neighbour = None
            else:
                self.neighbour = get_neighbour(self.ring, self.my_ip, 'left')

        self.logger.election(
            f"Ring formed: {' -> '.join(self.ring)} -> (wrap)",
            f"My neighbour (left): {self.neighbour}")

    def start_election(self, reason="unknown"):
        """Initiate election by sending own IP to left neighbour."""
        with self._lock:
            # Force reset — always allow a new election when explicitly called
            self.participant = True
            self.leader_ip = None
            self._election_event.clear()

        self.logger.election(
            f"*** ELECTION STARTED ***",
            f"Reason: {reason}, Ring size: {len(self.ring)}")

        if not self.neighbour or self.neighbour == self.my_ip or len(self.ring) <= 1:
            self._declare_self_leader()
            return

        # Send own ID to left neighbour (like lcr-template.py)
        election_message = {
            "mid": self.my_ip,
            "isLeader": False
        }
        self._send_to_neighbour(election_message)
        self.logger.election(f"Sent vote for self ({self.my_ip}) -> {self.neighbour}")

        # Start timeout thread
        threading.Thread(target=self._election_timeout, daemon=True).start()

    def _listen_loop(self):
        """Listen for election messages on the ring (like lcr-template.py)."""
        while self._running:
            try:
                data, address = self._ring_socket.recvfrom(BUFFER_SIZE)
                election_message = json.loads(data.decode())
                self._handle_election_message(election_message, address)
            except socket.timeout:
                continue
            except OSError:
                if self._running:
                    break
            except Exception as e:
                if self._running:
                    self.logger.fault(f"Ring listen error: {e}")

    def _handle_election_message(self, election_message, address):
        """
        Process received election message — core LCR logic.
        Adapted directly from lcr-template.py.
        """
        mid = election_message.get("mid", "")
        is_leader = election_message.get("isLeader", False)

        self.logger.election(
            f"Received ring message: mid={mid}, isLeader={is_leader}",
            f"From: {address[0]}")

        # If the sender or candidate is not in our ring, add them
        # (handles rejoining servers that we haven't discovered yet)
        with self._lock:
            ring_updated = False
            for ip in [mid, address[0]]:
                if ip and ip != self.my_ip and ip not in self.ring:
                    member_set = set(self.ring)
                    member_set.add(ip)
                    self.ring = form_ring(list(member_set))
                    self.neighbour = get_neighbour(self.ring, self.my_ip, 'left')
                    ring_updated = True
            if ring_updated:
                self.logger.election(
                    f"Ring updated from election message: {' -> '.join(self.ring)}",
                    f"New neighbour: {self.neighbour}")

        if is_leader:
            # Leader announcement — record and forward
            with self._lock:
                self.leader_ip = mid
                self.participant = False
            self._election_event.set()

            # Forward the leader announcement around the ring
            if mid != self.my_ip:
                self._send_to_neighbour(election_message)
                self.logger.election(
                    f"Leader announced: {mid}",
                    f"Forwarding announcement to {self.neighbour}")
            else:
                self.logger.election(
                    f"Leader announcement completed full ring")

            self._display_result()
            return

        # LCR comparison logic (from lcr-template.py)
        # Compare using binary IP representation for correct numeric ordering
        mid_bin = socket.inet_aton(mid)
        my_bin = socket.inet_aton(self.my_ip)

        if mid_bin < my_bin and not self.participant:
            # My IP is higher and I haven't participated — send my own
            with self._lock:
                self.participant = True
            new_election_message = {
                "mid": self.my_ip,
                "isLeader": False
            }
            self._send_to_neighbour(new_election_message)
            self.logger.election(
                f"Received {mid} < my IP {self.my_ip} (not yet participant)",
                f"Injecting own vote -> {self.neighbour}")

        elif mid_bin > my_bin:
            # Higher ID — forward it (it might be the leader)
            with self._lock:
                self.participant = True
            self._send_to_neighbour(election_message)
            self.logger.election(
                f"Received {mid} > my IP {self.my_ip}",
                f"Forwarding to {self.neighbour}")

        elif mid == self.my_ip:
            # My own ID came back — I am the leader!
            self._declare_self_leader()

        else:
            # mid < my_ip but already participant — drop
            self.logger.election(
                f"Received {mid} < my IP {self.my_ip} (already participant)",
                f"Dropping")

    def _declare_self_leader(self):
        """This node's ID completed full ring traversal — declare leadership."""
        with self._lock:
            self.leader_ip = self.my_ip
            self.participant = False
        self._election_event.set()

        self.logger.election(
            f"*** LEADER ELECTED: {self.my_ip} (ME) ***",
            f"My ID completed full ring traversal")

        # Send leader announcement around the ring
        leader_message = {
            "mid": self.my_ip,
            "isLeader": True
        }
        if self.neighbour:
            self._send_to_neighbour(leader_message)

        self._display_result()

    def _send_to_neighbour(self, message):
        """Send a JSON message to the left neighbour via UDP."""
        if not self.neighbour:
            return
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(json.dumps(message).encode(), (self.neighbour, RING_PORT))
            sock.close()
        except Exception as e:
            self.logger.fault(f"Failed to send to neighbour {self.neighbour}: {e}")

    def _election_timeout(self):
        """If election doesn't complete in time, shrink ring and retry."""
        if not self._election_event.wait(timeout=ELECTION_TIMEOUT):
            with self._lock:
                self.participant = False

            # Callback to server to get alive nodes and rebuild ring
            if self._get_alive_callback:
                alive_ips = self._get_alive_callback()
                self.logger.election(
                    f"Election timeout — checking alive nodes: {alive_ips}",
                    f"Old ring had {len(self.ring)} members")
                self.update_ring(alive_ips)
            else:
                # Fallback: remove current neighbour
                with self._lock:
                    if self.neighbour and self.neighbour != self.my_ip:
                        self.ring = [ip for ip in self.ring if ip != self.neighbour]
                        if len(self.ring) <= 1:
                            self.neighbour = None
                        else:
                            self.neighbour = get_neighbour(
                                self.ring, self.my_ip, 'left')

            self.start_election(reason="timeout (ring rebuilt)")

    def _display_result(self):
        """Display election result banner."""
        if self.leader_ip == self.my_ip:
            print(f"\n\033[92m{'='*60}")
            print(f"  ELECTION RESULT: I AM THE LEADER")
            print(f"{'='*60}")
            print(f"  Node IP:    {self.my_ip}")
            print(f"  Role:       LEADER (Sequencer + Coordinator)")
            print(f"  Ring:       {' -> '.join(self.ring)}")
            print(f"{'='*60}\033[0m\n")
        else:
            print(f"\n\033[93m{'='*60}")
            print(f"  ELECTION RESULT: LEADER IS {self.leader_ip}")
            print(f"{'='*60}")
            print(f"  Node IP:    {self.my_ip}")
            print(f"  Role:       FOLLOWER")
            print(f"  Leader:     {self.leader_ip}")
            print(f"  Ring:       {' -> '.join(self.ring)}")
            print(f"{'='*60}\033[0m\n")

    def is_leader(self):
        return self.leader_ip == self.my_ip

    def get_leader(self):
        return self.leader_ip

    def wait_for_leader(self, timeout=ELECTION_TIMEOUT):
        self._election_event.wait(timeout=timeout)
        return self.leader_ip
