"""
Reliable Multicast — ACK/NACK with retransmission over UDP.

Since UDP is unreliable, we add application-level reliability:
1. Leader sends message to all servers via UDP.
2. Each server ACKs back to the leader.
3. If no ACK within timeout, leader retransmits.
4. Receivers detect gaps in sequence numbers and NACK.
"""

import threading
import time

from common.constants import MESSAGE_RETRY_INTERVAL, MESSAGE_RETRY_MAX


class ReliableMulticast:
    """ACK/NACK-based reliable multicast over UDP."""

    def __init__(self, node_id, logger, send_callback):
        self.node_id = node_id
        self.logger = logger
        self.send_to_node = send_callback

        self.pending_acks = {}  # seq -> {targets, acked, msg, retries, time}
        self.message_log = {}  # seq -> msg (for retransmission)
        self.expected_sequence = 1
        self._lock = threading.Lock()
        self._running = False

    def start(self):
        self._running = True
        t = threading.Thread(target=self._retransmit_loop, daemon=True)
        t.start()
        self.logger.reliability("Reliable multicast started")

    def stop(self):
        self._running = False

    def track_message(self, seq, msg, targets):
        """Leader tracks a multicast message awaiting ACKs."""
        with self._lock:
            self.pending_acks[seq] = {
                "targets": set(targets),
                "acked": set(),
                "msg": msg,
                "retries": 0,
                "time": time.time(),
            }
            self.message_log[seq] = msg

        self.logger.reliability(
            f"Tracking Seq#{seq} — awaiting {len(targets)} ACKs",
            f"Targets: {list(targets)}")

    def handle_ack(self, seq, from_ip):
        """Process an ACK from a receiver."""
        with self._lock:
            if seq in self.pending_acks:
                self.pending_acks[seq]["acked"].add(from_ip)
                remaining = self.pending_acks[seq]["targets"] - self.pending_acks[seq]["acked"]
                if not remaining:
                    del self.pending_acks[seq]
                    self.logger.reliability(f"All ACKs received for Seq#{seq}")
                else:
                    self.logger.reliability(
                        f"ACK from {from_ip} for Seq#{seq}, waiting: {list(remaining)}")

    def _retransmit_loop(self):
        """Periodically retransmit unacknowledged messages."""
        while self._running:
            time.sleep(MESSAGE_RETRY_INTERVAL)
            now = time.time()

            with self._lock:
                to_retransmit = []
                to_remove = []

                for seq, entry in self.pending_acks.items():
                    if now - entry["time"] > MESSAGE_RETRY_INTERVAL:
                        if entry["retries"] >= MESSAGE_RETRY_MAX:
                            to_remove.append(seq)
                        else:
                            to_retransmit.append(seq)

                for seq in to_remove:
                    unacked = self.pending_acks[seq]["targets"] - self.pending_acks[seq]["acked"]
                    self.logger.fault(
                        f"Message Seq#{seq} delivery FAILED after {MESSAGE_RETRY_MAX} retries",
                        f"Unacked: {list(unacked)}")
                    del self.pending_acks[seq]

            for seq in to_retransmit:
                with self._lock:
                    if seq not in self.pending_acks:
                        continue
                    entry = self.pending_acks[seq]
                    entry["retries"] += 1
                    entry["time"] = now
                    unacked = entry["targets"] - entry["acked"]
                    msg = entry["msg"]

                for target in unacked:
                    self.logger.reliability(
                        f"Retransmitting Seq#{seq} to {target} (attempt {entry['retries']})")
                    self.send_to_node(target, msg)
