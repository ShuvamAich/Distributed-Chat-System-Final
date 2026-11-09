# DS Project Report Form — Answers

Fill these into the PDF form fields.

---

## PAGE 1: Project Information

**Group ID:** [Fill your group ID]

**Semester:** [Fill: e.g., WS25/26]

**Student Names:** [Fill your team members]

**Project Title:**
Distributed Real-Time Chat System (availability-first / AP)

**Project Description:**
A distributed real-time group chat with custom middleware (no Kafka/ZooKeeper).
Multiple servers form a peer-to-peer mesh; clients connect to any server via TCP.
The system is availability-first (AP): any server accepts a message, shows it to
its own users immediately, and gossips it peer-to-peer — there is no leader on the
message path. Delivery is causally ordered via vector clocks and converges to an
eventual total order via a Lamport-timestamp tiebreaker. An LCR-elected coordinator
handles only liveness and membership (control plane). Supports dynamic discovery,
leader election, network-partition tolerance, and decentralized gap recovery.

**Code Repository Link:**
https://github.com/ShuvamAich/Distributed-Chat-System-Final

---

## PAGE 1: Architectural Model

**Architecture Type:** [X] Hybrid

(Clients connect to servers via TCP = client-server. Servers form a peer-to-peer
mesh for messaging/replication and an LCR ring for voting = peer-to-peer.
Combined = hybrid.)

---

**Communication - TCP:**
Explain briefly what it is used for:
Client-to-server communication. Clients connect to a server via TCP (port 7004)
with 4-byte length-prefixed framing. TCP gives reliable, ordered delivery for join
requests, chat messages, catch-up sync, and disconnect notices.

**Communication - UDP:**
Explain briefly what it is used for:
All server-to-server communication: discovery broadcasts (5972), LCR ring election
votes (7001), peer-to-peer chat gossip and anti-entropy gap-fill (7002), and
heartbeats (7003). UDP is lightweight; reliability on the chat path comes from
decentralized anti-entropy rather than per-message ACKs.

**Communication - Other:**
Not applicable.

---

**Concurrency - Multithreading:**
Explain briefly what it is used for:
Each server runs ~10 threads: TCP accept loop, per-client handlers, discovery
broadcast/listen, LCR ring listener, heartbeat send/listen/check, the UDP chat+
anti-entropy listener, a periodic anti-entropy loop, a force-release loop, and
cluster management. The workload is I/O-bound (sockets), so threads sharing memory
(the message log, vector clock, connection pool) are efficient. The causal-delivery
engine guards its clocks + log + hold-back buffer with a single re-entrant lock.

**Concurrency - Multiprocessing:**
Not used. The workload is I/O-bound; threads share memory naturally without IPC
overhead.

---

## PAGE 2: System Architecture Diagram

```
+-------------+     UDP Broadcast (255.255.255.255:5972)     +-------------+
|  Client A   |<------------ Discovery ------------------>   |  Client B   |
| (Alice)     |                                              | (Bob)       |
+------+------+                                              +------+------+
       | TCP :7004                                                  | TCP :7004
       |                                                            |
+======|============================================================|======+
|      |            SERVER MESH (peer-to-peer, no leader hop)        |      |
|      v                                                            v      |
| +---------+   UDP :7002 chat gossip + anti-entropy   +---------+          |
| |Server 1 |<---------------------------------------->|Server 2 |          |
| |         |   UDP :7001 LCR ring votes (control)     |         |          |
| |         |<---------------------------------------->|         |          |
| |         |   UDP :7003 heartbeats (control)         |         |          |
| |         |<---------------------------------------->|         |          |
| +---------+                                          +---------+          |
|  Each server: vector clock, Lamport clock, append-only log, holdback buf  |
|  Elected coordinator = liveness + membership ONLY (OFF the message path)  |
+============================================================================+

Middleware Layer (custom, no ZooKeeper/Kafka):
[Discovery] [Election/LCR] [Heartbeat] [Ordering/CausalDelivery] [AntiEntropy]
[Group View]

Message path (NO leader):
- Client -> TCP -> its server
- Server stamps (vector clock + Lamport ts + unique id), shows to its own clients,
  and UDP-gossips peer-to-peer to every other server
- Each server delivers in causal order; replicas converge to one total order
- Anti-entropy periodically reconciles any gaps between peers
```

---

## PAGE 3: Dynamic Discovery of Hosts

**Discovery Mechanism:**
[X] Client discovers server
[X] Server discovers servers

(Servers broadcast their IP on UDP 5972 every 3 seconds. Clients listen to find a
server; servers listen to discover each other and form the mesh/ring. Servers not
heard from within a timeout are aged out.)

**Discovery Implemented:**
[X] Broadcast

(UDP broadcast to 255.255.255.255:5972, based on broadcastsender.py /
broadcastlistener.py. Broadcast reaches everyone in one packet with zero config;
its single-subnet limit matches the LAN scope.)

**Discovery Occurs:**
[X] When system starts
[X] Whenever new component comes in

(Continuous: servers broadcast every 3s. A new server is absorbed with no restart;
because the coordinator is off the message path, adding a server never pauses the
live chat — the newcomer catches up in the background via anti-entropy.)

---

## PAGE 3: Voting

**Voting Implemented Using:**
[X] LeLann-Chang-Roberts Algorithm

(Servers form a ring sorted by binary IP. Each node sends its IP around the ring;
the highest IP completes full traversal and declares itself leader. Based on
lcr-template.py and ring.py. The ring exists ONLY for voting — chat messages never
travel around it. A timeout rebuilds the ring if a vote is lost.)

**Group View Used:**
[X] Yes. Explain briefly what the group view is used for:
The group view tracks which servers are currently in the cluster and which clients
are connected locally. View changes occur when servers join, leave, or crash, and
they trigger re-election. It is used to pick gossip/mesh targets and the heartbeat
monitoring list. Note: membership is a control-plane concern — it never gates chat
delivery, which is peer-to-peer.

**Nodes Identified Using:**
[X] IP addresses / IP addresses + ports

(The LCR ring is sorted by binary IP via socket.inet_aton(); ports are fixed
constants 7001-7004. For ordering, each server's stable identity is its incarnation
"ip:epoch", and every message carries a globally-unique id "ip:epoch:counter" — no
central sequencer is needed.)

**Election Starts:**
[X] When system starts
[X] When a new server joins
[X] When the leader fails

(Triggers: initial formation, membership change via discovery, and heartbeat
timeout detecting a leader crash. Crucially, an in-progress election never freezes
the chat — only the coordinator's control-plane tasks wait.)

---

## PAGE 4: Fault Tolerance

**Faults Tolerated:**
[X] Crash Faults (Leader Server, Regular Server, Client)
[X] Omission Faults

(Crash faults: any server/client can die; the chat on every reachable side keeps
flowing, and a new coordinator is elected in the background. Omission faults: UDP
messages can be lost — handled by decentralized anti-entropy gap-fill, not central
retransmission. Byzantine faults are not handled.)

**Fault Detection:**
Explain who sends heartbeats to whom, their frequency, and retries:
The elected leader sends UDP heartbeats to all followers every 2 seconds (port
7003); followers ACK. Any message from a peer also counts as implicit liveness. If
a follower is silent beyond ~6 seconds the leader drops it from the ring; if the
leader is silent beyond ~6 seconds the followers declare it failed and start a new
LCR election. The timeout is conservative to avoid false alarms — and throughout
detection and re-election, the chat keeps running.

**Recovery Strategy:**
- Leader crash: followers detect via heartbeat timeout and re-elect among survivors.
  The chat is UNAFFECTED because the leader was never on the message path; only
  admission/membership resume after the new coordinator is chosen.
- Server crash: other servers keep serving their users instantly (no pause). On
  restart (a new incarnation "ip:epoch") the server catches up via anti-entropy.
  Clients on a dead server auto-reconnect to another server via discovery; their
  send buffer holds unsent text, and a last-seen marker fetches exactly what they
  missed — even when reconnecting to a different server.
- Network partition: each side keeps serving its reachable users and elects its own
  coordinator. Because the coordinator is off the message path, two coordinators
  cannot corrupt the chat. On heal, anti-entropy exchanges missed messages, views
  merge, a single coordinator is re-elected, and all replicas converge to the same
  total order. (Demonstrated with the server `partition <ip>` / `heal` commands.)
- No-loss scope: a message is safe once it has reached at least one other server;
  anti-entropy then spreads it everywhere. A message held only on one server that
  crashes before gossiping it can be lost (in-memory; full power-loss out of scope).

---

## PAGE 4: Reliable Ordered Multicast

**Type of Ordering:**
[X] Causal Ordering

**Reason for Chosen Ordering:**
In a group chat a reply must never appear before the message it answers, so we need
at least causal ordering (FIFO is too weak — it only orders per-sender). We
deliberately do NOT use immediate/blocking total ordering, because waiting for
global agreement before showing a message would cost availability during a
partition (CAP). Instead each message carries a vector clock and is shown only once
its causal dependencies are delivered. On top of causal order, every replica applies
the same deterministic (Lamport-timestamp, server-id) tiebreaker to concurrent
messages, so all replicas converge to one identical order — an *eventual* total
order that is a linear extension of the causal order (we never sort purely by
wall-clock time, which clock skew could get wrong).

**Reliability Mechanism:**
[X] Sequencing (vector clocks + per-message unique ids, decentralized — no leader seq#)
[X] Other. Explain briefly:

Decentralized, availability-preserving reliability:
1. Causal delivery: a vector clock holds a message until its dependencies arrive, so
   causality is never violated in the live view.
2. Force-release: if a dependency is genuinely lost, a held message is released after
   a timeout (a one-time ordering exception) so the system never blocks.
3. Anti-entropy gap-fill: servers periodically exchange a compact SACK-style digest
   (per origin: contiguous max + interior holes) and pull missing messages from any
   peer that holds them — no central confirm-and-resend authority. Duplicates are
   ignored by globally-unique message id.
4. Client send buffer: text typed while disconnected is queued and flushed on reconnect.
5. Cross-reconnect catch-up: the client sends a last-seen marker (a version vector) so
   it receives exactly the messages it missed, even on a different server.

**Implementation Details:**
Pure Python standard library (no external dependencies). Custom middleware with 6
modules: discovery (UDP broadcast), election (LCR ring via UDP), heartbeat (UDP
failure detection), ordering (CausalDelivery: vector clocks + Lamport tiebreaker +
hold-back buffer), antientropy (decentralized gap-fill), group_view (membership).
Server-to-server traffic uses UDP sockets; client-server uses TCP with 4-byte
length-prefixed framing. Every significant event is logged with timestamps for demo
visibility. Designed and tested across two machines on the same LAN, each acting as
a server with its own chat users.
