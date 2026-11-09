# Distributed Real-Time Chat System

## Complete Technical Documentation

An availability-first (AP) distributed group chat built entirely on the Python
standard library — no Kafka, no ZooKeeper. Every mechanism (discovery, voting,
ordering, reliability, fault tolerance) is implemented in a custom middleware
layer so each is visible and demonstrable.

---

## Table of Contents

1. System Overview
2. Architecture Model
3. Dynamic Discovery
4. Communication Model
5. Concurrency Model
6. Leader Election (LCR) — Control Plane Only
7. Coordination & Group View
8. Message Ordering (Causal → Eventual Total Order)
9. Reliability (Decentralized Anti-Entropy)
10. Fault Tolerance
11. System Architecture Diagram & Message Flow
12. Demo Guide
13. Design Rationale

---

## 1. System Overview

ResiChat is a distributed group chat that runs across several computers on one
LAN. Messages must reach everyone in a sensible (causal) order while the system
stays **available** — you can always chat with the people your side of the
network can reach, even when a server or the elected coordinator is down or being
re-elected.

**CAP positioning:** we prioritise **Availability + Partition-tolerance (AP)**.
We deliberately do *not* provide linearizability (which would force the chat to
pause during a partition). Instead we provide **causal consistency with eventual
convergence** — among the strongest guarantees compatible with always being
available, and more than enough for a chat.

### Key Properties

- **Zero configuration** — servers and clients find each other via UDP broadcast.
- **No leader on the message path** — any server accepts, shows, and gossips a
  message. There is no global sequence number and no mandatory coordinator hop.
- **Always available** — chat never blocks on an election, a leader, or a quorum.
- **Causal + convergent** — a reply is never shown before the message it answers;
  all replicas converge to one identical total order.
- **Partition-tolerant** — both sides keep chatting during a split and merge on heal.
- **Scalable** — nothing is hardcoded: no fixed server count, no user list.

### Components

```
common/
  constants.py      ports, timers, message-type strings
  vector_clock.py   causal-broadcast vector clock (keyed by "ip:epoch")
  lamport_clock.py  scalar logical clock for the total-order tiebreaker
  message.py        chat envelope schema + globally-unique msg_id + sort_key
  network.py        local-IP detection, broadcast addresses, ring helpers
  logger.py         rich color terminal logging (demo visibility)
middleware/
  discovery.py      UDP broadcast discovery
  election.py       LCR ring leader election (control plane only)
  heartbeat.py      UDP heartbeat crash detection
  ordering.py       CausalDelivery engine (vector clocks + Lamport + hold-back)
  antientropy.py    decentralized gap-fill (SACK-style digest exchange)
  group_view.py     membership bookkeeping
server/chat_server.py   server node: mesh messaging + control plane
client/chat_client.py   thin client: sends text, renders causal order
```

---

## 2. Architecture Model

### Choice: Hybrid (Client-Server + Peer-to-Peer Mesh)

- **Clients ↔ servers:** client-server over **TCP** (reliable, in-order link).
- **Servers ↔ servers:** a peer-to-peer **mesh** over **UDP**. Each server can
  exchange messages with every other directly.

Every server holds a **full replica** of the chat (an append-only log), so no
single node's loss takes the conversation down.

### Two topologies for two jobs

- **For voting:** the servers form a logical **ring** (sorted by binary IP),
  around which the LCR vote travels. This ring exists *only* for election.
- **For messaging & replication:** the servers form a **mesh** — messages are
  gossiped directly peer-to-peer and never travel around the ring.

### Roles

- **Server:** edge hub for its own TCP clients; peer in the mesh; participant in
  elections. Any server can become the coordinator.
- **Coordinator (elected leader):** performs **only** control-plane work —
  heartbeat liveness and the authoritative membership list. It is deliberately
  **off the message path**: it does not route, order, or relay chat messages.
- **Client:** thin. Sends raw text and renders whatever its server causally
  delivers. It assigns no clocks and no sequence numbers.

### Why not pure client-server?

A single central server is a single point of failure and a bottleneck; its loss
takes the whole chat down. The mesh keeps the conversation alive through crashes.

### Why not pure peer-to-peer among clients?

Clients come and go constantly and may sit behind NAT. A small set of servers
gives stable replicas and a clean discovery/ordering story while still avoiding a
single point of failure.

---

## 3. Dynamic Discovery

### Mechanism: UDP Broadcast (based on `broadcastsender.py` / `broadcastlistener.py`)

Each server periodically (every `DISCOVERY_INTERVAL` = 3 s) broadcasts an
announcement to `255.255.255.255:5972`; everyone listens and records who they
heard from and when. A new client finds a server; a new server is heard and
absorbed. No restart, no addresses typed in.

```
server: {"type": "DISCOVERY_ANNOUNCE", "ip": <ip>, "role": "SERVER", "tcp_port": 7004}
client: broadcasts {"type": "DISCOVERY_REQUEST"} and connects to the first responder
```

### Alive Detection

Servers not heard from within a freshness window (`DISCOVERY_INTERVAL * 3`) are
aged out of the membership view (`get_alive_servers`). Broadcast alone can't
reliably notice a silent crash, so the coordinator's heartbeats (§10) handle the
rest.

### Scaling out stays available

Because the coordinator is off the message path, adding a server never pauses the
live chat — existing users keep talking while the newcomer catches up in the
background via anti-entropy (§9). Its single limitation — staying on one subnet —
matches the LAN scope.

---

## 4. Communication Model

### Dual Protocol: UDP + TCP

- **TCP (client ↔ server):** reliable, ordered, with 4-byte length-prefixed
  framing. Carries join requests, chat text, catch-up sync, and disconnects.
- **UDP (server ↔ server):** lightweight datagrams for discovery, ring votes,
  heartbeats, peer-to-peer chat gossip, and anti-entropy. Loss on the chat path
  is repaired by anti-entropy, not by per-message ACKs.

### TCP Message Framing (client-server)

```
[ 4-byte big-endian length ][ JSON payload ]
```

### UDP Message Format (server-server)

Plain JSON dicts, e.g. a chat broadcast:

```
{"type": "CHAT_BCAST", "env": { ...message envelope... }}
```

### Port Allocation

| Port | Transport | Purpose |
|------|-----------|---------|
| 5972 | UDP | Discovery broadcast |
| 7001 | UDP | LCR election ring (control plane only) |
| 7002 | UDP | Peer-to-peer chat gossip + anti-entropy |
| 7003 | UDP | Heartbeats (control plane only) |
| 7004 | TCP | Client ↔ server |

---

## 5. Concurrency Model

### Choice: Multi-threading

The workload is almost entirely waiting on the network (I/O-bound), so
lightweight threads that share memory are the right fit, and Python's GIL releases
during socket I/O. Multiprocessing is avoided — it suits CPU-bound work and adds
IPC overhead for no benefit here.

### Thread Architecture (per server)

- TCP accept loop + one handler thread per connected client
- UDP chat/anti-entropy listener (port 7002)
- Discovery broadcast + listen
- LCR ring listener (port 7001)
- Heartbeat send / listen / check (port 7003)
- Anti-entropy periodic reconciliation loop
- Force-release loop (releases causally-stuck messages)
- Cluster-management loop (discovery → ring → election)

### Shared-state safety

The `CausalDelivery` engine guards its clocks + append-only log + hold-back buffer
with a single **re-entrant lock**. The critical section "check deliverability →
merge clock → deliver → re-scan hold-back" runs atomically and drains
iteratively; all network/TCP I/O happens *outside* the lock.

---

## 6. Leader Election (LCR Algorithm) — Control Plane Only

### Algorithm: LeLann-Chang-Roberts (based on `lcr-template.py` and `ring.py`)

Servers form a ring sorted by binary IP (`socket.inet_aton`). Each node sends its
IP (`mid`) around the ring; a node forwards only higher IDs and drops lower ones;
the node whose own ID returns full-circle declares itself leader (highest IP
wins). The leader announces itself with `isLeader: True` around the ring.

```
Message format: {"mid": <ip>, "isLeader": bool}
```

### Why LCR

Our servers already form a ring, so LCR reuses that structure with tiny logic
(forward the higher, drop the lower, declare self if your own vote returns). The
Bully algorithm tolerates a mid-election crash slightly better but needs
all-to-higher messaging and doesn't use our ring. Neither is safe alone, so we
pair LCR with a timeout that rebuilds the ring if a vote is lost.

### Election Triggers

- When the system starts
- When membership changes (a server joins or is aged out)
- When the leader fails (heartbeat timeout)

### Why the election can't freeze the chat

Election is a **control-plane** activity. If it stalls and the timeout rebuilds
the ring, the live chat is unaffected, because message delivery is peer-to-peer
and needs no current leader. Only the coordinator's tasks wait for the election —
a few seconds. This is exactly why we keep the leader off the message path.

### Ring auto-shrink on timeout

If the vote doesn't complete within `ELECTION_TIMEOUT` (5 s), the node asks for
the currently-alive members (via discovery), rebuilds the ring, and retries.

---

## 7. Coordination & Group View

Some tasks need a single decision-maker. The elected coordinator performs exactly
two control-plane jobs:

- **Checking servers are alive** — it sends periodic heartbeats and tracks who
  responds (§10).
- **Managing membership** — it keeps the authoritative list of which servers are
  in the cluster.

### What is the group view?

Membership bookkeeping: which servers are currently in the cluster and which
clients are connected locally. View changes (join/leave/crash) trigger
re-election and update the mesh gossip targets and heartbeat monitoring list.

### Why an elected coordinator, not a fixed one

A permanent coordinator would be a single point of failure. Any server can be
elected, and if it fails the others elect a new one — coordination always has an
owner without any server being irreplaceable.

### Why this doesn't hurt availability

The coordinator is off the message path. If it's briefly down or being
re-elected, the live chat keeps running; only the two coordination tasks pause.
Membership is a control-plane concern and **never gates chat delivery**.

---

## 8. Message Ordering (Causal → Eventual Total Order)

### Choice: Causal delivery (vector clocks) converging to an eventual total order

The standard guarantees form a hierarchy: FIFO ⊂ causal ⊂ total. Our live
delivery guarantee is **causal ordering** — a message is shown only after the
messages its sender had already seen, so a reply is never shown before the message
it answers (FIFO per-sender is included). On top of that, every replica applies
the same deterministic **(Lamport-timestamp, origin-id, origin-counter)**
tiebreaker, so all replicas converge to one identical order — an **eventual total
order**. What we deliberately avoid is immediate (blocking) total order: we never
wait for global agreement before showing a message, which is what would cost
availability.

### Two clocks, two jobs

- **Vector clock** (`common/vector_clock.py`), keyed by node incarnation
  `"ip:epoch"`: decides *live* causal deliverability. A message from sender `s`
  carrying clock `V` is deliverable at a node whose clock is `L` iff
  `V[s] == L[s]+1` and `V[k] <= L[k]` for all `k != s`. On delivery, `L` becomes
  the component-wise max of `L` and `V` (which bumps only `L[s]`). A node's own
  entry counts only messages it has *originated*; it is never advanced on receipt
  (this is the one place the naive "update + self-increment" vector clock would be
  wrong).

- **Lamport timestamp** (`common/lamport_clock.py`): a scalar updated on *every*
  delivery (`L = max(local, received) + 1`) and incremented on originate. It is
  the total-order tiebreaker.

### Why the Lamport sort is a linear extension of causal order

Because the Lamport clock is updated on every delivered message, a cause always
has a **strictly smaller** Lamport timestamp than its effect. Therefore sorting the
log by `(lamport_ts, origin_id, origin_counter)` can never place an effect before
its cause — the converged order is a *linear extension* of the causal order. The
`origin_id` breaks ties between concurrent messages (globally-unique IP-based id),
and `origin_counter` is a final defensive tiebreaker. We **never** sort by
wall-clock time, which clock skew could get wrong.

### Hold-back and force-release

An out-of-order message is held in a buffer until its dependencies arrive, then
delivered (and the buffer re-scanned, so a cascade releases in order). If a
dependency is genuinely lost (the originator crashed before anyone else saw it), a
held message is **force-released after `HOLDBACK_TIMEOUT` (4 s)** — a one-time
ordering exception that keeps the system live. Force-release appends the message
to the log and advances the Lamport clock but does **not** merge its vector clock,
so a late-arriving dependency is never mistaken for a duplicate and anti-entropy
can still fill the hole.

### Convergence invariant

The converged order is a pure function of the *set* of messages in the log. As
long as every replica ends up with the same set (guaranteed by `msg_id` dedup +
anti-entropy) and sorts by the same key, every replica shows the identical order.
The vector clock only governs live delivery before convergence.

### Display Format

Clients render `[LT<lamport>] <user>: <text>`. The server `history` command and
the client `/history` command print the converged total order
`[LT<lamport> <origin_id>#<counter>] <user>: <text>`.

---

## 9. Reliability (Decentralized Anti-Entropy)

There is no central confirm-and-resend authority. Reliability is built from
decentralized, availability-preserving layers:

### Layer 1 — Causal hold-back (ordering safety)

A vector clock holds a message until its dependencies arrive, so causality is
never violated in the live view (§8).

### Layer 2 — Force-release (liveness)

A genuinely orphaned message is released after a timeout so the system never
blocks on a lost dependency (§8).

### Layer 3 — Anti-entropy gap-fill (server-server)

Every `GAP_CHECK_INTERVAL` (4 s) each server picks a peer (round-robin) and sends
a compact **SACK-style digest**: per origin, the `contiguous_max` counter plus any
higher counters it already holds (`extras`). The peer computes exactly which
messages the sender is missing (`messages_missing_for`) and replies with them
(batched to fit a UDP datagram). Because every node gossips on its own timer,
reconciliation runs in both directions and the message sets converge. This fills
UDP losses, catches up a server that was down or partitioned, and drives
partition-heal convergence. Duplicates are ignored by globally-unique `msg_id`,
and a message can be pulled from *any* peer that holds it (so it survives the
originator crashing once one peer has it).

```
A --GAP_SUMMARY {from, digest}--> B     "here is what I have"
B --GAP_RESPONSE {from, messages}--> A  "here are the ones you were missing"
A feeds each returned envelope through CausalDelivery.on_receive (dedup + causal)
```

### Layer 4 — Client send buffer (client-side)

Text typed while disconnected is queued locally and flushed on reconnect.

### Layer 5 — Cross-reconnect catch-up (client-side)

The client keeps a per-origin **last-seen marker** (a version vector) and sends it
in its join request. The server replays exactly the messages the client missed —
even when the client reconnects to a *different* server, preserving causal context.

### Message identity

- `msg_id = "ip:epoch:counter"` — globally unique with no coordinator. The boot
  `epoch` makes ids restart-safe: a restarted server is a new incarnation, so it
  never reuses an id or regresses its own clock.
- `origin_id = "ip:epoch"` — the stable vector-clock key for that incarnation.

These replace the old leader-assigned sequence number entirely.

---

## 10. Fault Tolerance

Failure handling keeps the conversation available in every case; only
control-plane tasks ever wait.

### Crash detection: heartbeats (UDP)

The elected leader pings all followers every `HEARTBEAT_INTERVAL` (2 s) on port
7003; followers ACK. Any message from a peer also counts as implicit liveness. If
a node is silent beyond `HEARTBEAT_TIMEOUT` (~6 s) it is declared failed — a
follower loss drops it from the ring; a leader loss triggers a new LCR election.
The timeout is conservative to avoid false alarms, and the chat keeps running
throughout detection and re-election.

### Failure scenarios

**Scenario 1 — a client's server crashes.** The client's TCP connection drops; it
auto-reconnects to another server via discovery, its send buffer holds unsent
text, and its last-seen marker fetches exactly what it missed. Other servers keep
serving their users with no pause.

**Scenario 2 — the leader crashes.** Followers detect the heartbeat timeout and
re-elect among survivors. **The chat is unaffected** because the leader was never
on the message path; only liveness/membership resume after a new coordinator is
chosen.

**Scenario 3 — a non-leader server crashes.** The other servers keep serving
instantly. When it restarts (as a new `ip:epoch` incarnation) it catches up via
anti-entropy.

**Scenario 4 — network partition.** Each side keeps serving the users it can
reach and, having lost contact with the coordinator, elects its own. Because the
coordinator is off the message path, two coordinators cannot corrupt the chat —
they simply manage their own side's membership. The sides can't exchange messages
while split, so users on opposite sides temporarily don't see each other. On heal,
anti-entropy exchanges what each missed, membership views merge, a single
coordinator is re-elected, and all replicas converge to the same total order. We
stay available but not consistent across the split until it heals — the essence of
AP. (Demoable via the server `partition <ip>` / `heal` commands, which drop all
traffic to/from a peer across discovery, election, heartbeat, chat, and
anti-entropy.)

**Scenario 5 — a server is killed and restarted.** It rejoins discovery, is
re-absorbed into the ring, and anti-entropy brings its log up to date. Its new
incarnation id prevents any id/clock collision with its previous life.

### No message loss — scoped honestly

A message is safe once it has reached at least one other server; from there
anti-entropy spreads it everywhere. A message held only on a single server that
crashes before it gossips can still be lost, because history is in memory.
Full-cluster power loss is out of scope.

### Fault tolerance summary

| Fault | Detection | Recovery | Chat impact |
|-------|-----------|----------|-------------|
| Client disconnect | TCP close | reconnect + marker catch-up | none |
| Non-leader crash | heartbeat / age-out | anti-entropy on restart | none |
| Leader crash | heartbeat timeout | LCR re-election (background) | none |
| Network partition | heartbeat / discovery | per-side election; merge on heal | none (each side) |
| UDP packet loss | digest mismatch | anti-entropy pull | none |

---

## 11. System Architecture Diagram & Message Flow

```
+-------------+     UDP Broadcast (255.255.255.255:5972)     +-------------+
|  Client A   |<------------ Discovery ------------------>   |  Client B   |
+------+------+                                              +------+------+
       | TCP :7004                                                  | TCP :7004
       |                                                            |
+======|============================================================|======+
|      |            SERVER MESH (peer-to-peer, no leader hop)        |      |
|      v                                                            v      |
| +---------+  UDP :7002 chat gossip + anti-entropy   +---------+           |
| |Server 1 |<--------------------------------------->|Server 2 |           |
| |         |  UDP :7001 LCR ring votes (control)     |         |           |
| |         |  UDP :7003 heartbeats (control)         |         |           |
| +---------+                                         +---------+           |
|  Each server: vector clock, Lamport clock, append-only log, holdback buf  |
|  Elected coordinator = liveness + membership ONLY (OFF the message path)  |
+============================================================================+
```

### Message Flow (no leader)

```
1. Client A types text -> TCP -> its server S1
2. S1 stamps it: vector clock + Lamport ts + unique msg_id "ip:epoch:counter"
3. S1 delivers it to S1's own clients immediately
4. S1 UDP-gossips CHAT_BCAST to every other server (full mesh)
5. Each server delivers in causal order (holding if deps are missing)
6. Anti-entropy periodically reconciles any gaps between peers
7. Every replica sorts its log identically -> eventual total order
```

### Leader Failure & Re-Election Flow

```
Leader S3 crashes
  -> followers miss heartbeats > ~6s
  -> S1/S2 remove S3, rebuild ring, run LCR
  -> highest surviving IP becomes new coordinator
  -> MEANWHILE: chat between S1 and S2 never paused
```

---

## 12. Demo Guide

### Prerequisites

Python 3.7+ (no pip installs). On Windows: `set PYTHONIOENCODING=utf-8`.

### Logic tests

```
python test_local.py
```
Deterministic, network-free tests of causal hold-back, force-release liveness,
anti-entropy convergence, concurrent-message tiebreaker determinism, and
partition-merge convergence.

### Two-Machine Demo (recommended — both machines act as servers)

```
# Machine 1
python run_server.py
python run_client.py --username Alice

# Machine 2
python run_server.py
python run_client.py --username Bob
```
Both machines on the same LAN subnet. Servers discover each other and clients
discover a server automatically. (If they don't find each other, allow inbound UDP
5972/7001/7002/7003 and TCP 7004 on both.)

### Single-Machine Smoke Test (one server + clients)

```
# Terminal 1
python run_server.py --ip 127.0.0.1
# Terminal 2
python run_client.py --username Alice --server 127.0.0.1
# Terminal 3
python run_client.py --username Bob   --server 127.0.0.1
```
The full multi-server mesh is best shown across two real machines because
discovery and the mesh share fixed UDP ports.

### Demo Scenarios (the four required properties)

1. **Dynamic discovery** — start the servers; `status` on each shows both members.
   Start clients; they connect with no addresses typed.
2. **Voting (LCR)** — `status` shows the same elected leader (highest IP) on both.
3. **Fault tolerance** —
   - Kill a **non-leader** server: chats keep flowing; restart it and watch the
     `GOSSIP` logs catch it back up.
   - Kill the **leader**: chat keeps flowing; a new leader is elected in the
     background.
   - **Partition**: on each server type `partition <other-ip>`. Both sides keep
     chatting and each elects its own leader. Then `heal` on both — anti-entropy
     reconciles and `history` converges identically.
4. **Ordering** — delay a reply's cause to see causal hold-back; send concurrent
   messages from both machines, then run `history` on each server and confirm the
   converged order is identical.

### Commands

- **Server:** `status` · `history` · `partition <ip>` · `heal` · `quit`
- **Client:** `/status` · `/history` · `/quit`

### What to point out in the logs

- `DISCOVERY` lines — zero-config membership.
- `ELECTION` banner — LCR ring result, highest IP wins.
- `CAUSAL` / `MESSAGE` lines — a message stamped and gossiped with no leader hop.
- `FAULT` force-release line — liveness under genuine loss.
- `GOSSIP` lines — anti-entropy filling gaps and reconciling after a partition.

---

## 13. Design Rationale

- **Why AP, not CP?** CAP shows linearizability is incompatible with staying
  available during a partition — a linearizable chat would have to pause. Causal
  consistency with eventual convergence is among the strongest models compatible
  with always being available, and is more than enough for a chat.

- **Why keep the leader off the message path?** So that an election, a leader
  crash, or a partition never stalls the conversation. The leader is reduced to
  control-plane duties (liveness + membership) where a brief pause is acceptable.

- **Why vector clocks and not a single sequencer?** A single leader-assigned
  sequence number is a central ordering authority — it reintroduces the CP
  bottleneck we are avoiding. Vector clocks let every server order messages
  locally with no coordinator, and the Lamport tiebreaker makes the result
  converge to one identical total order.

- **Why anti-entropy instead of ACK/retransmit?** A leader-driven
  confirm-and-resend scheme is another central authority and another thing that
  stalls when the leader is gone. Decentralized digest exchange reconciles any two
  peers directly, doubles as new-server catch-up and partition-heal, and needs no
  coordinator.

- **Why `ip:epoch:counter` ids?** They are globally unique with no coordination,
  human-readable for the demo, double as the vector-clock key, and the boot epoch
  makes them restart-safe.

- **Limits (honest scope):** history is in memory and not trimmed; convergence
  assumes servers eventually reconnect; editing/deleting messages is out of scope
  (it would reintroduce the clashes this append-only design avoids); discovery is
  single-subnet (matching the LAN scope).
