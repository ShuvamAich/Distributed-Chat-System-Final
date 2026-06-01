"""
Lamport Clock implementation for logical ordering of messages.
Based on lamport_clock.py example code.

Rules:
1. Before sending: clock += 1, attach clock value to message
2. On receiving: clock = max(local_clock, received_clock) + 1
3. On local event: clock += 1
"""


class LamportClock:
    """Lamport logical clock (from lamport_clock.py example)."""

    def __init__(self):
        self.clock = 0

    def increment(self):
        """Increment before sending or on local event."""
        self.clock += 1
        return self.clock

    def update(self, received_ts):
        """Update on message receipt: max(local, received) + 1."""
        self.clock = max(self.clock, received_ts) + 1
        return self.clock

    def get(self):
        return self.clock

    def __repr__(self):
        return f"LamportClock({self.clock})"
