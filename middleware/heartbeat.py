"""
Heartbeat-based failure detection using UDP.

The leader sends HEARTBEAT to all servers via UDP.
Servers respond with HEARTBEAT_ACK.
If no heartbeat from leader within timeout, followers trigger re-election.
"""

import socket
import json
import threading
import time

from common.constants import (
    HEARTBEAT_PORT, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT,
    MSG_HEARTBEAT, MSG_HEARTBEAT_ACK, BUFFER_SIZE,
)


class HeartbeatService:
    """UDP-based heartbeat monitoring for crash detection."""

    def __init__(self, my_ip, logger, on_node_failed, on_leader_failed):
        self.my_ip = my_ip
        self.logger = logger
        self.on_node_failed = on_node_failed
        self.on_leader_failed = on_leader_failed
        self.monitored_nodes = {}  # ip -> last_ack_time
        self.leader_ip = None
        self.last_leader_heartbeat = time.time()
        self._lock = threading.Lock()
        self._running = False
        self._is_leader = False
        self._leader_failure_reported = False

    def start(self, is_leader=False, monitored_ips=None, leader_ip=None):
        self._running = True
        self._is_leader = is_leader
        self.leader_ip = leader_ip
        self._leader_failure_reported = False
        self.last_leader_heartbeat = time.time()

        if monitored_ips:
            with self._lock:
                for ip in monitored_ips:
                    self.monitored_nodes[ip] = time.time()

        # Start listener
        t = threading.Thread(target=self._listen_loop, daemon=True)
        t.start()

        if is_leader:
            t = threading.Thread(target=self._send_heartbeats_loop, daemon=True)
            t.start()

        t = threading.Thread(target=self._check_failures_loop, daemon=True)
        t.start()

        self.logger.heartbeat(
            f"Heartbeat service started",
            f"Role={'Leader' if is_leader else 'Follower'}, "
            f"Monitoring: {list(self.monitored_nodes.keys()) if monitored_ips else 'leader only'}")

    def stop(self):
        self._running = False

    def _listen_loop(self):
        """Listen for heartbeat messages via UDP."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.my_ip, HEARTBEAT_PORT))
        sock.settimeout(1.0)

        while self._running:
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode())
                msg_type = msg.get("type")

                if msg_type == MSG_HEARTBEAT:
                    # Received heartbeat from leader — send ACK
                    with self._lock:
                        self.last_leader_heartbeat = time.time()
                    ack = json.dumps({
                        "type": MSG_HEARTBEAT_ACK,
                        "from": self.my_ip
                    })
                    sock.sendto(ack.encode(), addr)

                elif msg_type == MSG_HEARTBEAT_ACK:
                    # Leader received ACK from a follower
                    sender_ip = msg.get("from", addr[0])
                    with self._lock:
                        if sender_ip in self.monitored_nodes:
                            self.monitored_nodes[sender_ip] = time.time()

            except socket.timeout:
                continue
            except OSError:
                break
            except Exception as e:
                if self._running:
                    self.logger.fault(f"Heartbeat listen error: {e}")

        sock.close()

    def _send_heartbeats_loop(self):
        """Leader sends heartbeats to all monitored nodes via UDP."""
        while self._running and self._is_leader:
            with self._lock:
                targets = list(self.monitored_nodes.keys())

            heartbeat_msg = json.dumps({
                "type": MSG_HEARTBEAT,
                "from": self.my_ip
            }).encode()

            for target_ip in targets:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.sendto(heartbeat_msg, (target_ip, HEARTBEAT_PORT))
                    sock.close()
                except Exception:
                    pass

            time.sleep(HEARTBEAT_INTERVAL)

    def _check_failures_loop(self):
        """Periodically check for failures."""
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            now = time.time()

            if self._is_leader:
                with self._lock:
                    failed = [ip for ip, last_ack in self.monitored_nodes.items()
                              if now - last_ack > HEARTBEAT_TIMEOUT]

                for ip in failed:
                    self.logger.fault(
                        f"NODE FAILURE DETECTED: {ip}",
                        f"Unresponsive for >{HEARTBEAT_TIMEOUT}s")
                    with self._lock:
                        if ip in self.monitored_nodes:
                            del self.monitored_nodes[ip]
                    self.on_node_failed(ip)
            else:
                with self._lock:
                    leader_timeout = (self.leader_ip is not None and
                                      not self._leader_failure_reported and
                                      now - self.last_leader_heartbeat > HEARTBEAT_TIMEOUT)

                if leader_timeout:
                    with self._lock:
                        self._leader_failure_reported = True
                        failed_leader = self.leader_ip
                        self.leader_ip = None
                    self.logger.fault(
                        f"LEADER FAILURE DETECTED: {failed_leader}",
                        f"Unresponsive for >{HEARTBEAT_TIMEOUT}s")
                    self.on_leader_failed()

    def update_monitored_nodes(self, node_ips):
        with self._lock:
            current = set(self.monitored_nodes.keys())
            new = set(node_ips)
            for ip in new - current:
                self.monitored_nodes[ip] = time.time()
            for ip in current - new:
                del self.monitored_nodes[ip]
