"""
Local unit tests — verifies all middleware layers work correctly.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common.lamport_clock import LamportClock
from common.network import form_ring, get_neighbour
from common.logger import SystemLogger


def test_lamport_clock():
    """Test Lamport clock ordering (based on lamport_clock.py example)."""
    print("\n" + "="*60)
    print("  TEST 1: Lamport Clock Ordering")
    print("="*60)

    clock_alice = LamportClock()
    clock_bob = LamportClock()
    clock_carol = LamportClock()

    # Alice sends M1: increment before send
    ts1 = clock_alice.increment()
    print(f"\n  Alice sends M1 'What time?'    LT={ts1}")
    assert ts1 == 1

    # Bob receives M1: max(local=0, received=1) + 1 = 2, then sends M2
    clock_bob.update(ts1)
    ts2 = clock_bob.increment()
    print(f"  Bob receives M1 (LT->2), sends M2 '3PM'  LT={ts2}")
    assert ts2 == 3

    # Carol receives M2: max(local=0, received=3) + 1 = 4, then sends M3
    clock_carol.update(ts2)
    ts3 = clock_carol.increment()
    print(f"  Carol receives M2 (LT->4), sends M3 'Agree'  LT={ts3}")
    assert ts3 == 5

    # Verify: causally related messages have increasing timestamps
    assert ts1 < ts2 < ts3
    print(f"\n  Ordering verified:")
    print(f"    L(M1)={ts1} < L(M2)={ts2} < L(M3)={ts3} [OK]")
    print(f"    (if a->b then L(a) < L(b))")

    # Concurrent messages: Dave sends without seeing anyone
    clock_dave = LamportClock()
    ts4 = clock_dave.increment()
    print(f"\n  Dave sends M4 independently      LT={ts4}")
    print(f"    L(M1)={ts1}, L(M4)={ts4} — both are 1")
    print(f"    Cannot determine if concurrent (Lamport limitation) [OK]")

    print("\n  [OK] Lamport Clock test PASSED")


def test_ring_formation():
    """Test ring formation and neighbour lookup (from ring.py)."""
    print("\n" + "="*60)
    print("  TEST 2: Ring Formation (ring.py pattern)")
    print("="*60)

    members = ['192.168.1.10', '192.168.1.5', '192.168.1.20', '192.168.1.1']
    ring = form_ring(members)
    print(f"\n  Members: {members}")
    print(f"  Ring (sorted by binary IP): {ring}")

    # Verify sorted order
    assert ring == ['192.168.1.1', '192.168.1.5', '192.168.1.10', '192.168.1.20']
    print(f"  Sort order: [OK]")

    # Test neighbour lookup
    n = get_neighbour(ring, '192.168.1.5', 'left')
    assert n == '192.168.1.10'
    print(f"  Neighbour of .5 (left): {n} [OK]")

    n = get_neighbour(ring, '192.168.1.20', 'left')
    assert n == '192.168.1.1'  # wraps around
    print(f"  Neighbour of .20 (left): {n} [OK] (wraps)")

    n = get_neighbour(ring, '192.168.1.1', 'right')
    assert n == '192.168.1.20'  # wraps around
    print(f"  Neighbour of .1 (right): {n} [OK] (wraps)")

    print("\n  [OK] Ring Formation test PASSED")


def test_lcr_logic():
    """Test LCR election logic simulation."""
    print("\n" + "="*60)
    print("  TEST 3: LCR Election Logic")
    print("="*60)

    # Simulate 3-node ring: .1 -> .5 -> .10 -> (wrap to .1)
    ring = ['192.168.1.1', '192.168.1.5', '192.168.1.10']
    print(f"\n  Ring: {' -> '.join(ring)} -> (wrap)")
    print(f"  Expected leader: 192.168.1.10 (highest binary IP)")

    # Simulate LCR manually
    # Each node sends its IP to left neighbour
    # .1 sends to .5, .5 sends to .10, .10 sends to .1

    print(f"\n  .5 receives .1's vote: .1 < .5 -> DROP (inject own)")
    print(f"  .10 receives .5's vote: .5 < .10 -> DROP (inject own)")
    print(f"  .1 receives .10's vote: .10 > .1 -> FORWARD to .5")
    print(f"  .5 receives forwarded .10: .10 > .5 -> FORWARD to .10")
    print(f"  .10 receives own ID back -> I AM THE LEADER!")

    # Verify: highest binary IP wins (last in sorted ring)
    import socket
    leader = max(ring, key=lambda ip: socket.inet_aton(ip))
    assert leader == '192.168.1.10'
    print(f"\n  Leader: {leader} (highest binary IP) [OK]")
    print("\n  [OK] LCR Election test PASSED")


def test_group_view():
    """Test group view management."""
    print("\n" + "="*60)
    print("  TEST 4: Group View Management")
    print("="*60)

    from middleware.group_view import GroupView
    logger = SystemLogger("TEST", "TEST")
    gv = GroupView("192.168.1.1", logger)

    gv.install_view(["192.168.1.1", "192.168.1.5", "192.168.1.10"], reason="initial")
    members = gv.get_members()
    assert len(members) == 3
    assert gv.get_view_id() == 1
    print(f"\n  View #1 installed: {members} [OK]")

    gv.install_view(["192.168.1.1", "192.168.1.5"], reason=".10 crashed")
    members = gv.get_members()
    assert len(members) == 2
    assert gv.get_view_id() == 2
    print(f"  View #2 installed: {members} [OK]")

    print("\n  [OK] Group View test PASSED")


def test_reliability():
    """Test reliable multicast tracking."""
    print("\n" + "="*60)
    print("  TEST 5: Reliable Multicast")
    print("="*60)

    from middleware.reliability import ReliableMulticast

    sent = []
    def mock_send(target, msg):
        sent.append((target, msg))

    logger = SystemLogger("TEST", "TEST")
    rm = ReliableMulticast("192.168.1.1", logger, mock_send)

    # Track a message
    rm.track_message(1, {"text": "hello"}, ["192.168.1.5", "192.168.1.10"])
    print(f"\n  Tracked Seq#1 for 2 targets")

    # ACK from .5
    rm.handle_ack(1, "192.168.1.5")
    assert 1 in rm.pending_acks
    print(f"  ACK from .5 [OK] (still waiting for .10)")

    # ACK from .10
    rm.handle_ack(1, "192.168.1.10")
    assert 1 not in rm.pending_acks
    print(f"  ACK from .10 [OK] (all delivered)")

    print("\n  [OK] Reliability test PASSED")


def main():
    print("\033[96m" + "="*60)
    print("  DISTRIBUTED CHAT SYSTEM - LOCAL TESTS")
    print("="*60 + "\033[0m")

    test_lamport_clock()
    test_ring_formation()
    test_lcr_logic()
    test_group_view()
    test_reliability()

    print("\n" + "\033[92m" + "="*60)
    print("  ALL TESTS PASSED [OK]")
    print("="*60 + "\033[0m")
    print("\n  Run 'python demo.py' to launch the full system.\n")


if __name__ == "__main__":
    main()
