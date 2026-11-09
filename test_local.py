"""
Logic tests for the AP causal-delivery + anti-entropy engine.

These are deterministic, network-free unit tests of the ordering guarantees the
system relies on. Run: python3 test_local.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from middleware.ordering import CausalDelivery
from common.message import sort_key
from common.vector_clock import VectorClock


class NullLogger:
    def __getattr__(self, _):
        return lambda *a, **k: None


class FakeClock:
    """Controllable wall clock so force-release timing is deterministic."""
    def __init__(self):
        self.t = 1000.0
    def __call__(self):
        return self.t
    def advance(self, dt):
        self.t += dt


def engine(node_id, clock=None, holdback=4.0):
    return CausalDelivery(node_id, NullLogger(), holdback, clock or (lambda: 1000.0))


def order_ids(eng):
    return [e["msg_id"] for e in eng.converged_order()]


passed = 0
def check(name, cond):
    global passed
    assert cond, f"FAILED: {name}"
    passed += 1
    print(f"  ok  {name}")


# --- Test 1: causal hold-back then ordered release -------------------------
def test_causal_holdback():
    A, B, C = engine("A:1"), engine("B:1"), engine("C:1")

    m1 = A.originate("alice", "hello")                 # A originates
    check("B delivers m1 immediately", B.on_receive(m1))
    m2 = B.originate("bob", "re: hello")               # causally after m1

    # C sees the reply (m2) BEFORE the cause (m1): must hold m2.
    delivered = C.on_receive(m2)
    check("C holds m2 (cause missing)", delivered == [])
    check("C holdback has 1", C.snapshot()["holdback"] == 1)

    # Now the cause arrives: m1 then m2 both deliver, in causal order.
    delivered = C.on_receive(m1)
    check("C releases both after cause", len(delivered) == 2)
    check("C order is m1 then m2",
          order_ids(C) == [m1["msg_id"], m2["msg_id"]])
    # Gossip m2 back to A so all three hold the same set, then compare.
    A.on_receive(m2)
    check("A/B/C converge identically",
          order_ids(A) == order_ids(B) == order_ids(C))


# --- Test 2: force-release keeps the system live --------------------------
def test_force_release():
    clk = FakeClock()
    C = engine("C:1", clock=clk)
    fabricated_cause_vc = {"Z:9": 1}
    orphan = {
        "msg_id": "Z:9:2", "origin_id": "Z:9", "origin_counter": 2,
        "username": "zoe", "text": "orphan", "system": False,
        "vector_clock": {"Z:9": 2}, "lamport_ts": 5, "wall_time": 1000.0,
    }
    check("orphan is held", C.on_receive(orphan) == [])
    check("nothing released before timeout", C.force_release_due() == [])
    clk.advance(5.0)
    released = C.force_release_due()
    check("orphan force-released after timeout", len(released) == 1)
    check("orphan now in log", orphan["msg_id"] in order_ids(C))


# --- Test 3: anti-entropy digest names interior holes ---------------------
def test_antientropy_digest():
    full = engine("S:1")
    m1 = full.originate("u", "one")
    m2 = full.originate("u", "two")
    m3 = full.originate("u", "three")

    gapped = engine("T:1")
    gapped.on_receive(m1)
    gapped.on_receive(m3)   # m3 depends on m2 -> held, so only m1 is delivered

    d = gapped.digest()
    check("gapped contiguous_max is 1", d["S:1"]["contiguous_max"] == 1)

    missing = full.messages_missing_for(d)
    missing_ids = {e["msg_id"] for e in missing}
    check("full knows gapped needs m2", m2["msg_id"] in missing_ids)

    # Deliver the fill; now the held m3 should cascade.
    for e in missing:
        gapped.on_receive(e)
    check("gapped now has all three",
          order_ids(gapped) == [m1["msg_id"], m2["msg_id"], m3["msg_id"]])


# --- Test 4: concurrent messages get a deterministic total order ----------
def test_tiebreaker_determinism():
    # Two independent origins, concurrent (neither sees the other).
    A, B = engine("A:1"), engine("B:1")
    ma = A.originate("alice", "from A")
    mb = B.originate("bob", "from B")
    check("ma and mb are concurrent",
          VectorClock.are_concurrent(ma["vector_clock"], mb["vector_clock"]))

    # Replica X receives A-then-B; replica Y receives B-then-A.
    X, Y = engine("X:1"), engine("Y:1")
    X.on_receive(ma); X.on_receive(mb)
    Y.on_receive(mb); Y.on_receive(ma)
    check("X and Y converge to the same order", order_ids(X) == order_ids(Y))
    check("tiebreak is by (lamport, origin, counter)",
          order_ids(X) == [e["msg_id"] for e in sorted([ma, mb], key=sort_key)])


# --- Test 5: partition heal — two divergent sides converge ----------------
def test_partition_merge():
    # Side 1 and Side 2 each chat independently during a split.
    s1, s2 = engine("S1:1"), engine("S2:1")
    a = s1.originate("alice", "s1 msg 1")
    b = s1.originate("alice", "s1 msg 2")
    c = s2.originate("carol", "s2 msg 1")
    d = s2.originate("carol", "s2 msg 2")

    # Heal: exchange everything via anti-entropy (both directions).
    for e in s1.messages_missing_for(s2.digest()):
        s2.on_receive(e)
    for e in s2.messages_missing_for(s1.digest()):
        s1.on_receive(e)

    check("both sides hold all 4 messages",
          len(order_ids(s1)) == 4 and len(order_ids(s2)) == 4)
    check("both sides converge to identical order", order_ids(s1) == order_ids(s2))


if __name__ == "__main__":
    print("Running AP engine logic tests...\n")
    test_causal_holdback()
    test_force_release()
    test_antientropy_digest()
    test_tiebreaker_determinism()
    test_partition_merge()
    print(f"\nAll {passed} checks passed.")
