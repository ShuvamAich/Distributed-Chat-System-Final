"""
Causal-delivery engine for the AP chat system.

Replaces the old leader-sequencer. There is NO global sequence number and NO
leader on the message path. Each server runs one CausalDelivery instance that:

  * stamps locally-originated messages with a vector clock + Lamport timestamp,
  * delivers incoming peer messages in CAUSAL order (holding a message until its
    dependencies arrive), force-releasing after a timeout so it never blocks,
  * keeps an append-only log that every replica sorts identically into the
    eventual total order via (lamport_ts, origin_id, origin_counter).

Convergence rule of thumb: correctness of the *converged* order depends only on
every replica ending up with the same SET of messages in the log (guaranteed by
msg_id dedup + anti-entropy) and sorting them by the same key. The vector clock
only governs *live* delivery order before convergence.

Threading: one RLock guards clocks + log + holdback. The "check deliverability ->
merge -> deliver -> re-scan holdback" cascade runs entirely inside the lock and
drains iteratively (never recursively). Callers perform network/TCP I/O on the
returned envelopes OUTSIDE the lock.
"""

import threading

from common.lamport_clock import LamportClock
from common.vector_clock import VectorClock
from common.message import make_envelope, sort_key


class CausalDelivery:
    def __init__(self, node_id, logger, holdback_timeout, wall_clock):
        self.node_id = node_id          # "ip:epoch"
        self.logger = logger
        self._holdback_timeout = holdback_timeout
        self._now = wall_clock          # callable -> float (injectable for tests)

        self.vc = VectorClock(node_id)
        self.lamport = LamportClock()

        self._log = []                  # delivered envelopes, in delivery order
        self._ids = set()               # delivered msg_ids (dedup key)
        self._by_origin = {}            # origin_id -> {counter: env}
        self._holdback = []             # [(arrival_time, env)] awaiting dependencies
        self._lock = threading.RLock()

    # --- Originate (this server's own client typed a message) ---

    def originate(self, username, text, system=False):
        with self._lock:
            self.vc.increment()
            counter = self.vc.own()
            ts = self.lamport.increment()
            env = make_envelope(
                origin_id=self.node_id, counter=counter, username=username,
                text=text, vector_clock=self.vc.get(), lamport_ts=ts,
                wall_time=self._now(), system=system)
            self._record(env)
        self.logger.causal(
            f"Originated \"{text}\" as {env['msg_id']}",
            f"VC={env['vector_clock']} LT={ts}")
        return env

    # --- Receive a peer message (returns envelopes newly ready to show) ---

    def on_receive(self, env):
        with self._lock:
            if env["msg_id"] in self._ids:
                return []                       # already have it — ignore duplicate
            if any(h["msg_id"] == env["msg_id"] for _, h in self._holdback):
                return []                       # already waiting on it
            self._holdback.append((self._now(), env))
            return self._drain()

    def _drain(self):
        """Deliver every holdback message whose causal deps are now satisfied."""
        delivered = []
        progress = True
        while progress:
            progress = False
            for i, (_, env) in enumerate(self._holdback):
                if self.vc.is_deliverable(env["origin_id"], env["vector_clock"]):
                    self._holdback.pop(i)
                    self.vc.merge(env["vector_clock"])
                    self._deliver(env)
                    delivered.append(env)
                    progress = True
                    break
        return delivered

    def force_release_due(self):
        """Release messages stuck past the hold-back timeout (dependency lost).

        We append them to the log and advance the Lamport clock, but DO NOT merge
        their vector clock — that would mark still-missing dependencies as seen and
        make a late-arriving dependency look like a duplicate, causing permanent
        divergence. The hole stays open for anti-entropy to fill; dedup is by msg_id.
        """
        released = []
        with self._lock:
            now = self._now()
            overdue = [(t, e) for (t, e) in self._holdback
                       if now - t >= self._holdback_timeout]
            overdue.sort(key=lambda te: sort_key(te[1]))
            for t, env in overdue:
                self._holdback.remove((t, env))
                self._deliver(env)
                released.append(env)
        for env in released:
            self.logger.fault(
                f"Force-released {env['msg_id']} (dependency never arrived)",
                "one-time ordering exception — staying live")
        return released

    # --- internal: commit an envelope to the log (clock already handled) ---

    def _deliver(self, env):
        self.lamport.update(env["lamport_ts"])
        self._record(env)

    def _record(self, env):
        self._ids.add(env["msg_id"])
        self._log.append(env)
        self._by_origin.setdefault(env["origin_id"], {})[env["origin_counter"]] = env

    # --- Views for clients / anti-entropy ---

    def converged_order(self):
        """The eventual total order — identical on every replica with the same set."""
        with self._lock:
            return sorted(self._log, key=sort_key)

    def digest(self):
        """SACK-style summary: per origin, the contiguous max plus any higher ids held."""
        with self._lock:
            out = {}
            for origin, by_counter in self._by_origin.items():
                counters = set(by_counter)
                cmax = 0
                while (cmax + 1) in counters:
                    cmax += 1
                extras = sorted(c for c in counters if c > cmax)
                out[origin] = {"contiguous_max": cmax, "extras": extras}
            return out

    def messages_missing_for(self, peer_digest):
        """Envelopes THIS node holds that the peer (described by peer_digest) lacks."""
        with self._lock:
            missing = []
            for origin, by_counter in self._by_origin.items():
                pd = peer_digest.get(origin, {"contiguous_max": 0, "extras": []})
                peer_has = set(range(1, pd["contiguous_max"] + 1)) | set(pd["extras"])
                for counter, env in by_counter.items():
                    if counter not in peer_has:
                        missing.append(env)
            return missing

    def missed_for_client(self, client_marker):
        """Envelopes (in converged order) newer than a client's last-seen marker.

        client_marker maps origin_id -> highest contiguous counter the client saw.
        Works across reconnects to a different server (causal context preserved).
        """
        with self._lock:
            marker = client_marker or {}
            return [e for e in sorted(self._log, key=sort_key)
                    if e["origin_counter"] > marker.get(e["origin_id"], 0)]

    def snapshot(self):
        with self._lock:
            return {
                "delivered": len(self._log),
                "holdback": len(self._holdback),
                "vc": self.vc.get(),
                "lamport": self.lamport.get(),
            }
