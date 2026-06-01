"""
Message Ordering — Total Ordering with Lamport Timestamps.
Based on lamport_clock.py example code.

Our ordering model:
- Total ordering via leader-assigned sequence numbers (primary)
- Lamport timestamps for logical time tracking (secondary)

The leader assigns both:
1. A monotonic sequence number (1, 2, 3...) for total ordering
2. A Lamport timestamp for logical clock synchronization

Clients maintain their own Lamport clocks:
- Increment before sending
- Update (max + 1) on receiving

This ensures L(cause) < L(effect) for all causally related messages,
though concurrent messages may have any Lamport ordering.
"""

from common.lamport_clock import LamportClock


class OrderingService:
    """Total ordering via sequencer + Lamport timestamps."""

    def __init__(self, node_id, logger):
        self.node_id = node_id
        self.logger = logger
        self.lamport_clock = LamportClock()
        self._sequence_counter = 0

    def assign_sequence(self):
        """Leader assigns next global sequence number."""
        self._sequence_counter += 1
        self.logger.order(f"Assigned sequence #{self._sequence_counter}")
        return self._sequence_counter

    def tick(self):
        """Increment Lamport clock (local event or send)."""
        return self.lamport_clock.increment()

    def on_receive(self, received_ts):
        """Update Lamport clock on message receipt."""
        if received_ts:
            self.lamport_clock.update(received_ts)
        return self.lamport_clock.get()
