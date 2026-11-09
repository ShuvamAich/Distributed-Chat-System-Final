"""
Chat message envelope for the AP distributed chat system.

Messages travel as plain JSON dicts (matching the UDP/TCP transport used
everywhere else). This module is the single source of truth for the envelope
shape and for globally-unique message IDs.

Envelope fields:
    msg_id        "<ip>:<epoch>:<counter>" — globally unique, the dedup key
    origin_id     "<ip>:<epoch>"           — the originating node incarnation
    origin_counter int                     — == origin's own vector-clock entry
    username      str                       — display name of the human sender
    text          str                       — message body
    system        bool                      — True for join/leave notices
    vector_clock  {origin_id: int}          — causal dependencies
    lamport_ts    int                       — scalar for the eventual total order
    wall_time     float                     — informational only, never used for ordering
"""


def make_msg_id(origin_id, counter):
    """Build the globally-unique message id from a node incarnation + counter."""
    return f"{origin_id}:{counter}"


def make_envelope(origin_id, counter, username, text, vector_clock,
                  lamport_ts, wall_time, system=False):
    """Assemble a chat message envelope (a plain JSON-serializable dict)."""
    return {
        "msg_id": make_msg_id(origin_id, counter),
        "origin_id": origin_id,
        "origin_counter": counter,
        "username": username,
        "text": text,
        "system": system,
        "vector_clock": vector_clock,
        "lamport_ts": lamport_ts,
        "wall_time": wall_time,
    }


def sort_key(env):
    """Deterministic eventual-total-order key: (lamport, origin, counter).

    Lamport timestamps are consistent with causality, so a cause always has a
    strictly smaller lamport_ts than its effect; origin_id breaks ties between
    concurrent messages and origin_counter is a final defensive tiebreaker.
    """
    return (env["lamport_ts"], env["origin_id"], env["origin_counter"])
