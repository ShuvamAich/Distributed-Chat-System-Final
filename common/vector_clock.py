"""
Vector clock for causal ordering of messages (causal broadcast).

Each node keeps one entry per known node incarnation (keyed by "ip:epoch").
A node's own entry counts only the messages it has ORIGINATED.

Causal-broadcast delivery rule — a message from sender s carrying clock V is
deliverable at a node whose clock is L iff:
    V[s] == L[s] + 1              (it is the very next message we expect from s)
    V[k] <= L[k]  for all k != s  (we have already delivered every dependency)
After delivery, L becomes the component-wise max of L and V. Because the rule
guarantees V[s] == L[s]+1 and V[k] <= L[k] elsewhere, that max bumps only L[s]
and never spuriously advances our own entry.
"""


class VectorClock:
    """Vector clock for establishing causal ordering between events."""

    def __init__(self, node_id):
        self.node_id = node_id
        self.clock = {node_id: 0}

    def increment(self):
        """Bump this node's own entry — call ONLY when originating a message."""
        self.clock[self.node_id] = self.clock.get(self.node_id, 0) + 1
        return dict(self.clock)

    def own(self):
        """This node's own counter (== number of messages it has originated)."""
        return self.clock.get(self.node_id, 0)

    def is_deliverable(self, sender_id, other_clock):
        """Causal-broadcast check for a message from `sender_id` carrying `other_clock`."""
        if other_clock.get(sender_id, 0) != self.clock.get(sender_id, 0) + 1:
            return False
        for k, v in other_clock.items():
            if k == sender_id:
                continue
            if v > self.clock.get(k, 0):
                return False
        return True

    def merge(self, other_clock):
        """Absorb a delivered message's clock: component-wise max, no self-increment."""
        for k, v in other_clock.items():
            self.clock[k] = max(self.clock.get(k, 0), v)

    def bump(self, node_id):
        """Advance a single entry by one (used by force-release to open a hole)."""
        self.clock[node_id] = self.clock.get(node_id, 0) + 1

    def get(self):
        return dict(self.clock)

    @staticmethod
    def is_causally_before(vc_a, vc_b):
        """True if vc_a causally precedes vc_b (vc_a < vc_b)."""
        all_keys = set(vc_a) | set(vc_b)
        at_least_one_less = False
        for k in all_keys:
            a, b = vc_a.get(k, 0), vc_b.get(k, 0)
            if a > b:
                return False
            if a < b:
                at_least_one_less = True
        return at_least_one_less

    @staticmethod
    def are_concurrent(vc_a, vc_b):
        """True if neither clock causally precedes the other."""
        return (not VectorClock.is_causally_before(vc_a, vc_b) and
                not VectorClock.is_causally_before(vc_b, vc_a))

    def __repr__(self):
        return f"VectorClock({self.clock})"
