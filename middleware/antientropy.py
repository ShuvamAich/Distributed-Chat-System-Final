"""
Anti-entropy (peer gap-fill) for the AP chat system.

Replaces the old leader-driven ACK/retransmit reliability layer. There is no
central confirm-and-resend authority. Instead every server periodically picks a
peer and gossips a compact SACK-style digest of what it holds; the peer replies
with any messages the sender lacks. This fills UDP losses, catches up a server
that was down or partitioned, and drives partition-heal convergence — all
decentralized.

Protocol (all over the chat UDP port):
  A --MSG_GAP_SUMMARY {from, digest}--> B     "here is what I have"
  B --MSG_GAP_RESPONSE {from, messages}--> A  "here are the ones you were missing"
  A feeds each returned envelope through CausalDelivery.on_receive (dedup + causal).

Because every node sends summaries on its own timer, reconciliation happens in
both directions and the message sets converge.
"""

import threading
import time

from common.constants import MSG_GAP_SUMMARY, MSG_GAP_RESPONSE

RESPONSE_BATCH = 50  # cap messages per gap-response to stay within a UDP datagram


class AntiEntropy:
    def __init__(self, my_ip, logger, delivery, send_cb, peers_cb, interval):
        self.my_ip = my_ip
        self.logger = logger
        self.delivery = delivery
        self._send = send_cb        # send_cb(target_ip, msg_dict) -> UDP to CHAT_PORT
        self._peers = peers_cb      # peers_cb() -> list[str] of reachable peer IPs
        self._interval = interval
        self._running = False
        self._rr = 0                # round-robin cursor over peers

    def start(self):
        self._running = True
        threading.Thread(target=self._loop, daemon=True).start()
        self.logger.gossip("Anti-entropy service started",
                           f"Reconciling every {self._interval}s")

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            time.sleep(self._interval)
            peers = self._peers()
            if not peers:
                continue
            # Round-robin so every peer is reconciled with over time.
            peer = peers[self._rr % len(peers)]
            self._rr += 1
            self._send(peer, {
                "type": MSG_GAP_SUMMARY,
                "from": self.my_ip,
                "digest": self.delivery.digest(),
            })

    # --- Called by the server's UDP listener on inbound gossip ---

    def handle_summary(self, peer_ip, peer_digest):
        """A peer told us what it has; send back whatever it is missing."""
        missing = self.delivery.messages_missing_for(peer_digest)
        if not missing:
            return
        missing.sort(key=lambda e: (e["origin_id"], e["origin_counter"]))
        for i in range(0, len(missing), RESPONSE_BATCH):
            batch = missing[i:i + RESPONSE_BATCH]
            self._send(peer_ip, {
                "type": MSG_GAP_RESPONSE,
                "from": self.my_ip,
                "messages": batch,
            })
        self.logger.gossip(
            f"Gap-fill -> {peer_ip}: sent {len(missing)} message(s) it lacked")

    def handle_response(self, messages):
        """We received messages we were missing; deliver them causally."""
        delivered = []
        for env in messages:
            delivered.extend(self.delivery.on_receive(env))
        if messages:
            self.logger.gossip(
                f"Gap-fill received {len(messages)} message(s)",
                f"{len(delivered)} became deliverable")
        return delivered
