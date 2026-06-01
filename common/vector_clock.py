"""
Vector Clock implementation for causal ordering of messages.
Each node maintains a vector of logical timestamps — one entry per known node.
"""


class VectorClock:
    """Vector clock for establishing causal ordering between events."""

    def __init__(self, node_id):
        self.node_id = node_id
        self.clock = {}

    def increment(self):
        """Increment this node's entry before sending a message."""
        self.clock[self.node_id] = self.clock.get(self.node_id, 0) + 1
        return dict(self.clock)

    def update(self, other_clock):
        """Merge with received clock: take component-wise max, then increment own."""
        for node_id, ts in other_clock.items():
            self.clock[node_id] = max(self.clock.get(node_id, 0), ts)
        self.clock[self.node_id] = self.clock.get(self.node_id, 0) + 1

    def get(self):
        return dict(self.clock)

    @staticmethod
    def is_causally_before(vc_a, vc_b):
        """Returns True if vc_a causally precedes vc_b (vc_a < vc_b)."""
        all_keys = set(list(vc_a.keys()) + list(vc_b.keys()))
        at_least_one_less = False
        for k in all_keys:
            a_val = vc_a.get(k, 0)
            b_val = vc_b.get(k, 0)
            if a_val > b_val:
                return False
            if a_val < b_val:
                at_least_one_less = True
        return at_least_one_less

    @staticmethod
    def are_concurrent(vc_a, vc_b):
        """Returns True if neither clock causally precedes the other."""
        return (not VectorClock.is_causally_before(vc_a, vc_b) and
                not VectorClock.is_causally_before(vc_b, vc_a))

    def __repr__(self):
        return f"VectorClock({self.clock})"
