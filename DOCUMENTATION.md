# Distributed Real-Time Chat System

## Complete Technical Documentation

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Model](#2-architecture-model)
3. [Dynamic Discovery](#3-dynamic-discovery)
4. [Communication Model](#4-communication-model)
5. [Concurrency Model](#5-concurrency-model)
6. [Leader Election (LCR Algorithm)](#6-leader-election-lcr-algorithm)
7. [Group View Communication](#7-group-view-communication)
8. [Message Ordering](#8-message-ordering)
9. [Reliability Mechanism](#9-reliability-mechanism)
10. [Fault Tolerance](#10-fault-tolerance)
11. [System Architecture Diagram](#11-system-architecture-diagram)
12. [Demo Guide](#12-demo-guide)
13. [Design Rationale](#13-design-rationale)

---

## 1. System Overview

This is an industry-grade distributed real-time chat system built entirely with custom middleware (no ZooKeeper, no Kafka, no external coordination services). The system allows multiple users to communicate in real-time across multiple machines with guaranteed message ordering, reliability, and fault tolerance.

Based on fundamental distributed systems patterns demonstrated in the example code (`broadcastsender.py`, `broadcastlistener.py`, `lcr-template.py`, `ring.py`, `simpleserver.py`, `simpleclient.py`, `lamport_clock.py`).

### Key Properties

| Property | Implementation |
|----------|---------------|
| Architecture | Hybrid (Client-Server + P2P Server Ring) |
| Discovery | UDP Broadcast (`255.255.255.255:5972`) — `broadcastsender.py` pattern |
| Server Communication | UDP (ring election, heartbeats, chat replication) |
| Client Communication | TCP with length-prefixed framing — `simpleclient.py` pattern |
| Concurrency | Multi-threading (I/O bound) |
| Leader Election | LCR Ring Algorithm — `lcr-template.py` pattern |
| Ring Formation | Binary IP sort — `ring.py` pattern |
| Message Ordering | Lamport Timestamps (`lamport_clock.py` pattern) + Total Ordering (Leader Sequencer) |
| Reliability | ACK/NACK + Client send buffer + Server pending queue |
| Fault Tolerance | Heartbeat-based crash detection + auto re-election + message replication + reconnect with gap recovery |
| Group Membership | View-change protocol |

### Components

```
distributed_chat/
├── common/                  # Shared utilities
│   ├── constants.py         # System-wide configuration & ports
│   ├── message.py           # Message protocol (serialization)
│   ├── network.py           # form_ring(), get_neighbour(), get_local_ip()
│   ├── logger.py            # Rich terminal logging with timestamps
│   ├── lamport_clock.py      # Lamport clock implementation (from lamport_clock.py example)
│   └── vector_clock.py      # Vector clock (unused — kept for reference/comparison)
├── middleware/              # Custom middleware layer
│   ├── discovery.py         # UDP broadcast discovery
│   ├── election.py          # LCR ring election via UDP
│   ├── heartbeat.py         # UDP heartbeat failure detection
│   ├── ordering.py          # Causal ordering service
│   ├── reliability.py       # ACK/NACK reliable multicast
│   ├── group_view.py        # Membership management
│   └── transport.py         # (reference — sockets used directly per component)
├── server/
│   └── chat_server.py       # Server node implementation
├── client/
│   └── chat_client.py       # Client implementation
├── Example Code/            # Reference patterns from course
├── demo.py                  # Interactive demo launcher
├── run_server.py            # Quick server launch
├── run_client.py            # Quick client launch
└── test_local.py            # Unit tests
```

---

## 2. Architecture Model

### Choice: Hybrid (Client-Server + Peer-to-Peer)

**Why Hybrid?**

A pure client-server model (like WhatsApp) has a single point of failure. A pure P2P model (like Tox) struggles with ordering and consistency. Our hybrid combines the best of both:

- **Client-Server aspect:** Clients connect to servers via TCP. Servers handle message sequencing, storage, and delivery. This simplifies client logic and enables features like history sync.

- **Peer-to-Peer aspect:** Servers form a ring topology and communicate as equals via UDP. Any server can become the leader via LCR election. There is no permanently designated "master."

### Roles

| Role | Responsibilities |
|------|-----------------|
| **Server (any)** | Accept client TCP connections, replicate messages, monitor cluster health |
| **Server (leader)** | All of the above + sequence messages (assign global order), coordinate view changes |
| **Client** | Discover servers, connect via TCP, send/receive messages, buffer on disconnect |

### Why Not Pure Client-Server?

- Single coordinator = single point of failure
- No automatic failover
- Clients can't migrate if their server dies

### Why Not Pure P2P?

- Total ordering requires a sequencer
- Managing N² connections is expensive
- Client devices may be unreliable/mobile

---

## 3. Dynamic Discovery

### Mechanism: UDP Broadcast (based on `broadcastsender.py` / `broadcastlistener.py`)

**How it works:**

1. Servers periodically broadcast announcements on `255.255.255.255:5972`
2. All nodes (servers and clients) listen on this broadcast port
3. When a new node hears an announcement from an unknown peer, it registers that peer
4. Discovery cache tracks `last_seen` timestamps for alive-detection

**Server broadcast code pattern (from `broadcastsender.py`):**

```python
broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
announcement = json.dumps({"type": "DISCOVERY_ANNOUNCE", "ip": my_ip, "role": "SERVER"})
broadcast_socket.sendto(str.encode(announcement), ("255.255.255.255", 5972))
```

**Client listener pattern (from `broadcastlistener.py`):**

```python
listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listen_socket.bind(("0.0.0.0", 5972))
data, addr = listen_socket.recvfrom(4096)
# Parse announcement, connect to discovered server
```

**Why UDP Broadcast for Discovery?**

- **Zero configuration:** No need to know any IP addresses in advance
- **Automatic:** New nodes are found instantly without restart
- **Efficient:** Single packet reaches all LAN nodes
- **Standard pattern:** Same as `broadcastsender.py` example

### Alive Detection

The discovery cache stores `last_seen` timestamps. The `get_alive_servers(max_age)` method returns only servers that have broadcast within the last `3 * DISCOVERY_INTERVAL` seconds. This is used during election timeouts to quickly identify which nodes are actually still running.

---

## 4. Communication Model

### Dual Protocol: UDP + TCP

| Purpose | Protocol | Why | Based On |
|---------|----------|-----|----------|
| Discovery | UDP Broadcast | Fire-and-forget, reaches all LAN | `broadcastsender.py` |
| Election ring | UDP point-to-point | Fast, matches LCR model | `lcr-template.py` |
| Heartbeats | UDP point-to-point | Lightweight, frequent | `simpleserver.py` |
| Server-to-server chat | UDP point-to-point | Fast replication | `simpleserver.py` |
| Client-to-server | TCP | Reliable, ordered delivery | `simpleclient.py` |

### TCP Message Framing (Client-Server)

All TCP messages use length-prefixed framing:

```
┌──────────────┬──────────────────────────┐
│ 4 bytes      │ N bytes                  │
│ (msg length) │ (JSON payload)           │
└──────────────┴──────────────────────────┘
```

### UDP Message Format (Server-Server)

Direct JSON encoding, no framing needed (UDP is message-oriented):

```python
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.sendto(json.dumps(message).encode(), (target_ip, port))
```

### Port Allocation

| Port | Purpose |
|------|---------|
| 5972 | UDP Broadcast discovery |
| 10001 | UDP LCR ring election |
| 10002 | UDP chat messages (server-to-server) |
| 10003 | UDP heartbeats |
| 10004 | TCP client connections |

---

## 5. Concurrency Model

### Choice: Multi-threading

**Why Multi-threading (not Multiprocessing)?**

Our workload is I/O-bound (network communication), not CPU-bound:

1. **Shared memory:** All threads access the same message log, connection pool, and state
2. **Lower overhead:** Thread creation is lighter than process creation
3. **GIL is not a problem:** Python's GIL releases during I/O operations (socket reads/writes)
4. **Simpler coordination:** Locks and events vs pipes/queues

### Thread Architecture (per server)

| Thread | Responsibility |
|--------|---------------|
| Main | Command input (status/quit) |
| TCP-Accept | Accept incoming client connections |
| TCP-Client-{n} | One per connected client, reads messages |
| Discovery-Broadcast | Periodic broadcast announcements |
| Discovery-Listen | Listen for broadcast messages |
| Ring-Listen | LCR election message listener (UDP) |
| Heartbeat-Listen | Receive heartbeats and ACKs (UDP) |
| Heartbeat-Send | Leader sends heartbeats periodically |
| Heartbeat-Check | Monitor for failures |
| Chat-Listen | Receive server-to-server messages (UDP) |
| Cluster-Join | Discover peers, trigger elections |

---

## 6. Leader Election (LCR Algorithm)

### Algorithm: LeLann-Chang-Roberts (based on `lcr-template.py` and `ring.py`)

**Ring Formation (from `ring.py`):**

```python
def form_ring(members):
    sorted_binary_ring = sorted([socket.inet_aton(member) for member in members])
    sorted_ip_ring = [socket.inet_ntoa(node) for node in sorted_binary_ring]
    return sorted_ip_ring

def get_neighbour(ring, current_node_ip, direction='left'):
    current_node_index = ring.index(current_node_ip)
    if direction == 'left':
        return ring[(current_node_index + 1) % len(ring)]
```

IPs are sorted by binary representation. Each node's "left neighbour" is the next node clockwise.

**Election Protocol (from `lcr-template.py`):**

```python
# Message format: {"mid": ip_address, "isLeader": bool}

if election_message['mid'] < my_ip and not participant:
    # My IP is higher — inject my own vote
    participant = True
    send_to_neighbour({"mid": my_ip, "isLeader": False})

elif election_message['mid'] > my_ip:
    # Higher ID — forward it
    participant = True
    send_to_neighbour(election_message)

elif election_message['mid'] == my_ip:
    # My ID came back — I am the leader!
    send_to_neighbour({"mid": my_ip, "isLeader": True})
```

**IP comparison uses binary representation** (`socket.inet_aton`) for correct numeric ordering (e.g., `192.168.1.10` > `192.168.1.5`).

### Election Triggers

| Trigger | Action |
|---------|--------|
| Cluster formation | First stable discovery → election |
| Membership change | New server joins or leaves → re-election |
| Leader heartbeat timeout | Follower detects failure → re-election |
| Election timeout | No leader in 5s → shrink ring (remove dead nodes) → retry |

### Ring Auto-Shrink on Timeout

When an election times out (votes not reaching anyone), the system:
1. Calls `_get_alive_members()` — queries discovery cache for recently-broadcasting nodes
2. Rebuilds the ring with ONLY alive nodes
3. If only self is alive → ring size 1 → self-elects immediately

This handles scenarios where multiple servers die simultaneously — the survivor converges to self-leadership within one timeout period (5s), not one timeout per dead node.

### Dynamic Ring Update from Election Messages

If a node receives a vote from an IP not in its current ring (e.g., a server that restarted), it dynamically adds that IP to the ring. This ensures rejoining servers can participate in elections without waiting for the full discovery cycle.

### Example Election (3 servers)

```
Ring: 127.0.0.1 -> 127.0.0.2 -> 127.0.0.3 -> (wrap)
Highest binary IP: 127.0.0.3

Step 1: Each node sends its IP to left neighbour
  .1 → "127.0.0.1" → .2
  .2 → "127.0.0.2" → .3
  .3 → "127.0.0.3" → .1

Step 2: Comparisons
  .2 receives "127.0.0.1": .1 < .2 → DROP, inject own "127.0.0.2" → .3
  .3 receives "127.0.0.2": .2 < .3 → DROP, inject own "127.0.0.3" → .1
  .1 receives "127.0.0.3": .3 > .1 → FORWARD to .2

Step 3: Forwarding
  .2 receives "127.0.0.3": .3 > .2 → FORWARD to .3

Step 4: Victory
  .3 receives "127.0.0.3": MY ID! → I AM THE LEADER
  .3 sends {"mid": "127.0.0.3", "isLeader": true} around ring
```

---

## 7. Group View Communication

### What is a View?

A "view" is a snapshot of group membership. It tracks which servers are currently in the cluster and which clients are connected.

### View Changes

| Event | Trigger | Action |
|-------|---------|--------|
| Server Join | Discovery finds new server | Add to ring, re-elect |
| Server Crash | Heartbeat timeout | Remove from ring, update view |
| Server Shutdown | Graceful SHUTDOWN message | Remove, update heartbeat targets |
| Client Join | JOIN_REQUEST received | Add to client list, announce |
| Client Leave | DISCONNECT or TCP failure | Remove, announce |

### Consistency

All servers store the same `chat_history` (replicated via leader multicast). When a new server joins, the leader's history is authoritative. When clients reconnect, they receive missed messages from whichever server they connect to.

---

## 8. Message Ordering

### Choice: Lamport Timestamps + Total Ordering (Leader Sequencer)

### Lamport Clock (from `lamport_clock.py` example)

The `lamport_clock.py` example demonstrates logical timestamps for ordering events across processes. We use Lamport clocks directly — each participant maintains a single integer counter.

**Lamport Clock Rules (from example code):**

```python
def local_event(pid, clock):
    clock += 1                              # Rule 1: increment on local event
    return clock

def send_event(pipe, pid, clock):
    clock += 1                              # Rule 2: increment before sending
    pipe.send((pid, clock))
    return clock

def receive_event(pipe, pid, clock):
    sender_id, ts = pipe.recv()
    clock = max(ts, clock) + 1              # Rule 3: max(local, received) + 1
    return clock
```

**Who maintains Lamport clocks:**

| Participant | Clock | Actions |
|-------------|-------|---------|
| Client (Alice) | Single integer | Increment on send, update on receive |
| Client (Bob) | Single integer | Increment on send, update on receive |
| Leader server | Single integer | Update from client's TS, then increment |

**Synchronization flow:**

```
Alice sends "Hi"
  → Alice increments: LT = 1
  → Message carries ts = 1
  → Server (leader) receives: LT = max(server_LT=0, client_ts=1) + 1 = 2
  → Ordered message sent to all with ts = 2

Bob receives message
  → Bob updates: LT = max(bob_LT=0, received_ts=2) + 1 = 3

Bob sends "Hey"
  → Bob increments: LT = 4
  → Server receives: LT = max(server_LT=2, client_ts=4) + 1 = 5
  → Ordered message sent to all with ts = 5
  → Alice receives: LT = max(alice_LT=1, received_ts=5) + 1 = 6
```

**What Lamport timestamps guarantee:** If event A causally precedes event B, then `L(A) < L(B)`. However, `L(A) < L(B)` does NOT guarantee A caused B — concurrent events may have any ordering.

### Total Ordering (Leader Sequencer)

The leader assigns a monotonic **sequence number** to each message. This is the PRIMARY ordering mechanism:

- **Seq#1, Seq#2, Seq#3...** — strictly monotonic, gap-free
- All participants deliver messages in seq# order
- Same sequence seen by every client → total ordering

This provides:
- Consistent replication (all servers store messages in the same order)
- Gap detection (clients know if they missed messages via `last_seq`)
- History replay (reconnecting clients request messages after their `last_seq`)

### Display Format

Messages show both ordering mechanisms:
```
[14:30:01] [Seq#1|LT:2] Alice: Hello
[14:30:02] [Seq#2|LT:5] Bob: Hi there
```

Where:
- `Seq#` = Total order (from leader's sequence counter)
- `LT:` = Lamport timestamp (logical time synchronized across all participants)

### Why Lamport + Total (not Vector Clocks)?

| Aspect | Lamport Clock | Vector Clock |
|--------|--------------|-------------|
| Space per message | Single integer (`ts: 5`) | Dict of all participants (`vc: {Alice:3, Bob:2, Leader:5}`) |
| Proves causality | Only `L(A) < L(B)` if A→B | Full: can prove A→B, A‖B (concurrent) |
| Detects concurrency | No | Yes |
| Complexity | Trivial | Grows with participant count |
| Needed for our system? | Total order comes from seq# anyway | Overkill — seq# already gives total order |

**Decision:** Since total ordering is guaranteed by the leader's sequence number, we don't need vector clocks to prove causality. Lamport timestamps provide logical time tracking with minimal overhead. The seq# is the source of truth for ordering.

### Why Not FIFO Only?

FIFO guarantees Alice's messages arrive in order relative to each other, but says nothing about cross-sender ordering. If Alice asks "What time?" and Bob replies "3 PM", FIFO doesn't guarantee all clients see Alice's question before Bob's answer.

Our system provides **Total Ordering** (stronger than FIFO):
```
FIFO ⊂ Causal ⊂ Total
```

Every participant sees every message in the exact same global order.

### Ordering During Faults

| Fault | Impact | Mechanism |
|-------|--------|-----------|
| Non-leader dies | No impact — seq# continues | Leader alive, counter uninterrupted |
| Leader dies | Brief pause during election | New leader reads max(seq) from chat_history, continues from there |
| Multiple servers die | Same as leader dies | Survivor's history has all replicated messages |
| Messages during election | Queued in `_pending_queue` | Flushed with next seq# after new leader established |

---

## 9. Reliability Mechanism

### Multi-Layer Reliability

The system uses three complementary mechanisms to guarantee message delivery:

### Layer 1: Client Send Buffer (Client-Side)

When a client's server dies or the connection breaks:

```
Alice types "Hello" → server unreachable
  → message queued in _send_buffer
  → client auto-reconnects to another server
  → on successful join: _flush_send_buffer() sends all queued messages
  → messages delivered in order
```

**Implementation:**
- Messages are buffered as dicts in `_send_buffer` list
- On reconnect + JOIN_APPROVED: background thread flushes buffer
- If flush fails partway through, remaining messages stay buffered

### Layer 2: Server Pending Queue (Server-Side)

When there's no leader (election in progress), servers queue incoming messages:

```
Bob sends "Hi" → server has no leader (election happening)
  → message queued in _pending_queue
  → election completes → new leader established
  → _flush_pending_queue() processes all queued messages
  → messages get sequenced and delivered normally
```

**Implementation:**
- Messages stored in `_pending_queue` with text, username, vc, system flag
- After `_post_election_setup()`: queue flushed through normal `_process_chat_message`

### Layer 3: ACK/NACK Reliable Multicast (Server-Server)

Leader tracks delivery to other servers:

```
Leader sends Seq#5 to S1, S2
  → tracks pending ACKs: {targets: {S1, S2}, acked: {}}
  → S1 sends ACK → acked: {S1}
  → S2 doesn't ACK within 1s → RETRANSMIT to S2
  → S2 ACKs → fully delivered
  → After 5 retries: declare S2 failed
```

### Layer 4: Gap Recovery on Reconnect (Client-Side)

When a client reconnects, it tells the server its `last_received_seq`:

```
Alice reconnects after crash
  → JOIN_REQUEST includes last_seq=42
  → Server checks chat_history for messages with seq > 42
  → Sends only missed messages (not full history)
  → Alice is caught up without duplicates
```

### Combined Flow (All Layers Active)

```
Normal:     Client → TCP → Server → Leader → Multicast → All Servers → TCP → Clients
Disconnect: Client → buffer | Server → pending_queue
Recovery:   Reconnect → flush buffer → gap recovery → caught up
```

---

## 10. Fault Tolerance

### Crash Detection: Heartbeat-Based (UDP)

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Heartbeat interval | 2s | Fast detection while low overhead |
| Heartbeat timeout | 30s | Tolerates temporary network issues |
| Election timeout | 5s | Fast convergence after failure |
| Discovery interval | 3s | Quick detection of new/returning nodes |

### Failure Scenarios

#### Scenario 1: Client's Server Crashes (e.g., Alice on S1, S1 dies)

```
1. Alice's TCP connection drops (immediate detection)
2. Client enters reconnect mode
3. Messages typed during disconnect → buffered in _send_buffer
4. Client discovers another server via broadcast
5. Connects, sends JOIN with last_seq=N
6. Server sends missed messages (seq > N)
7. Client flushes send buffer → queued messages delivered
8. System announcement: "Alice reconnected"
```

**No messages lost.** Buffer + gap recovery = complete delivery.

#### Scenario 2: Leader Crashes

```
1. All followers detect heartbeat timeout (30s)
2. _on_leader_failed triggers:
   a. Heartbeat stopped
   b. Discovery cache checked for alive nodes
   c. Dead leader removed from ring
   d. Ring rebuilt with only alive nodes
   e. New election started
3. During election: incoming messages queued in _pending_queue
4. Election completes (highest alive IP wins)
5. New leader starts heartbeating
6. _flush_pending_queue → queued messages sequenced and delivered
```

**Downtime:** ~30s (heartbeat timeout) + ~5s (election) = ~35s. Messages during this window are queued, not lost.

#### Scenario 3: Multiple Servers Crash Simultaneously

```
Example: 5 servers, kill 3 (including leader)

1. Surviving followers detect leader timeout
2. _on_leader_failed → checks get_alive_servers()
3. Discovery cache shows only 2 alive nodes
4. Ring rebuilt with 2 nodes → election → highest wins
5. If election times out (neighbour also dead):
   → _get_alive_callback() → recheck alive nodes
   → Ring rebuilt with only truly alive nodes
   → Self-elect if alone
```

**Key mechanism:** Election timeout triggers `_get_alive_callback()` which queries the discovery cache for nodes that broadcast within the last 9 seconds. All dead nodes are removed in ONE shot, not one per timeout.

#### Scenario 4: Server Temporarily Unreachable (Network Partition)

```
1. Leader stops receiving heartbeat ACKs from server X
2. After 30s: leader declares X failed, removes from ring
3. If X comes back: broadcasts discovery announcement
4. Other servers' _cluster_join_loop detects membership change
5. Re-election triggered → cluster reforms with X included
6. X receives missed messages from the leader's chat_history
```

#### Scenario 5: Server Killed and Restarted

```
1. S3 is killed
2. Remaining servers detect failure, re-elect
3. S3 restarts, broadcasts discovery
4. Existing servers detect membership change → re-election
5. If S3 has highest IP → becomes leader again
6. Chat history is rebuilt as messages flow through new leader
```

**Ring auto-update:** When a restarted server sends election votes to existing servers, they dynamically add it to their ring even before the `_cluster_join_loop` discovers it.

### Fault Tolerance Summary Table

| Failure | Detection | Recovery | Message Loss |
|---------|-----------|----------|--------------|
| Client's server dies | TCP disconnect (immediate) | Auto-reconnect + buffer flush + gap recovery | None |
| Leader dies | Heartbeat timeout (30s) | Re-election + pending queue flush | None (queued) |
| Multiple servers die | Heartbeat + election timeout | Alive-check + ring shrink + self-elect | None (queued) |
| Client disconnects | TCP failure on server | Remove from client list | Recoverable on reconnect |
| Network partition | Heartbeat timeout | Re-election in majority partition | Queued during partition |
| Server restarts | Discovery broadcast | Ring auto-update + re-election | History replayed |

---

## 11. System Architecture Diagram

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                         DISTRIBUTED CHAT SYSTEM                              ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ┌───────────────── UDP BROADCAST (255.255.255.255:5972) ────────────┐      ║
║  │  Servers broadcast announcements periodically                      │      ║
║  │  Clients listen to discover servers                                │      ║
║  └──────────┬───────────────────┬──────────────────┬─────────────────┘      ║
║             │                   │                  │                         ║
║  ┌──────────▼────┐   ┌─────────▼─────┐   ┌───────▼───────┐                 ║
║  │   Client A    │   │   Client B    │   │   Client C    │                 ║
║  │  (Alice)      │   │  (Bob)        │   │  (Carol)      │                 ║
║  │               │   │               │   │               │                 ║
║  │ • Lamport Clk │   │ • Lamport Clk │   │ • Lamport Clk │                 ║
║  │ • Send Buffer │   │ • Send Buffer │   │ • Send Buffer │                 ║
║  │ • Auto-Recon  │   │ • Auto-Recon  │   │ • Auto-Recon  │                 ║
║  │ • Gap Recovery│   │ • Gap Recovery│   │ • Gap Recovery│                 ║
║  └───────┬───────┘   └───────┬───────┘   └───────┬───────┘                 ║
║          │TCP :10004          │TCP :10004          │TCP :10004              ║
║          │                    │                    │                         ║
║  ╔═══════╪════════════════════╪════════════════════╪══════════════════╗      ║
║  ║       │        SERVER CLUSTER (LCR Ring)        │                  ║      ║
║  ║       │                    │                    │                  ║      ║
║  ║  ┌────▼─────┐        ┌────▼─────┐        ┌────▼─────┐            ║      ║
║  ║  │Server .1 │◄─UDP──►│Server .2 │◄─UDP──►│Server .3 │            ║      ║
║  ║  │(Follower)│ :10001 │(Follower)│ :10001 │ (LEADER) │            ║      ║
║  ║  │          │        │          │        │          │            ║      ║
║  ║  │•Replicate│        │•Replicate│        │•Sequencer│            ║      ║
║  ║  │•ChatHist │        │•ChatHist │        │•ChatHist │            ║      ║
║  ║  │•PendQueue│        │•PendQueue│        │•PendQueue│            ║      ║
║  ║  └──────────┘        └──────────┘        └──────────┘            ║      ║
║  ║       ▲                    ▲                    ▲                  ║      ║
║  ║       └─── Heartbeat :10003 ──── Chat :10002 ──┘                  ║      ║
║  ╚═══════════════════════════════════════════════════════════════════╝      ║
║                                                                              ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  MIDDLEWARE LAYER (Custom - No ZooKeeper/Kafka)                              ║
║  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐         ║
║  │Discovery │ │Election  │ │Heartbeat │ │ Ordering │ │Reliability│         ║
║  │(UDP Bcast)│ │(LCR/UDP) │ │  (UDP)   │ │(Lamport) │ │(ACK/Retry)│         ║
║  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘         ║
║  ┌──────────┐                                                                ║
║  │Group View│                                                                ║
║  │(Members) │                                                                ║
║  └──────────┘                                                                ║
╚═══════════════════════════════════════════════════════════════════════════════╝
```

### Message Flow Diagram

```
Alice (Client)        Server S1 (Follower)     Server S3 (Leader)        Server S2        Bob (Client)
    │                        │                        │                      │                │
    │─TCP─CHAT_MSG──────────►│                        │                      │                │
    │  VC={Alice:1}          │                        │                      │                │
    │                        │─UDP─forward────────────►│                      │                │
    │                        │                        │                      │                │
    │                        │                        │ assign seq=1         │                │
    │                        │                        │ merge VC, increment  │                │
    │                        │                        │ VC={Alice:1,Ldr:1}   │                │
    │                        │                        │                      │                │
    │                        │◄─UDP─CHAT_ORDERED──────│──UDP─CHAT_ORDERED───►│                │
    │                        │  seq=1                 │  seq=1               │                │
    │                        │                        │                      │                │
    │◄─TCP─CHAT_ORDERED──────│                        │                      │─TCP─ORDERED───►│
    │  seq=1                 │                        │                      │  seq=1         │
    │                        │─UDP─ACK────────────────►│◄─────UDP─ACK────────│                │
```

### Leader Failure & Re-Election Flow

```
S1 (Follower)              S3 (Leader - DIES)           S2 (Follower)
    │                            │                          │
    │◄───heartbeat───────────────│──────heartbeat──────────►│
    │                            │                          │
    │                          ╳ CRASH ╳                    │
    │                                                       │
    │  ... 30s no heartbeat ...                             │  ... 30s no heartbeat ...
    │                                                       │
    │  LEADER FAILURE DETECTED                              │  LEADER FAILURE DETECTED
    │  check alive: [S1, S2]                                │  check alive: [S1, S2]
    │  update ring: [S1, S2]                                │  update ring: [S1, S2]
    │                                                       │
    │──────── LCR Election (S2 > S1 → S2 wins) ───────────►│
    │                                                       │
    │  LEADER IS S2                                         │  I AM THE LEADER
    │  flush pending queue                                  │  flush pending queue
    │                                                       │  start heartbeating S1
```

---

## 12. Demo Guide

### Prerequisites

- Python 3.7+ (no external dependencies)
- Multiple machines on the same LAN (for multi-machine demo)
- OR: multiple terminal windows on one machine (loopback addresses)
- Windows: `set PYTHONIOENCODING=utf-8`

### Multi-Machine Demo (3 computers, same LAN)

```bash
# Machine 1:
python run_server.py

# Machine 2:
python run_server.py

# Machine 3:
python run_server.py

# Wait ~10s for discovery + election

# Any machine:
python run_client.py --username Alice

# Any machine:
python run_client.py --username Bob
```

### Single-Machine Demo (loopback)

```bash
# Terminal 1:
python run_server.py --ip 127.0.0.1

# Terminal 2:
python run_server.py --ip 127.0.0.2

# Terminal 3:
python run_server.py --ip 127.0.0.3

# Terminal 4:
python run_client.py --username Alice --server 127.0.0.1

# Terminal 5:
python run_client.py --username Bob --server 127.0.0.2
```

### Demo Scenarios

| # | Scenario | What to do | What to observe |
|---|----------|-----------|-----------------|
| 1 | Cluster formation | Start servers one by one | Discovery logs, ring formation, LCR votes, leader banner |
| 2 | Message exchange | Type messages as Alice and Bob | Sequence numbers, Lamport timestamps, delivery to both clients |
| 3 | Server failure | Kill a non-leader server | Client reconnects, buffered messages sent, gap recovery |
| 4 | Leader failure | Kill the leader | Heartbeat timeout, re-election, pending queue flushed |
| 5 | Multiple failures | Kill multiple servers | Alive-check, ring shrink, self-election |
| 6 | Server restart | Kill and restart a server | Discovery, ring auto-update, re-election |
| 7 | Message during election | Send messages while leader is down | Messages buffered, delivered after election |

### Commands

**Server:** `status` (show cluster state), `quit` (graceful shutdown)

**Client:** `/status` (connection info + Lamport clock), `/history` (last 20 messages), `/quit` (leave)

### What to Point Out in Logs

| Log Category | Demonstrates |
|--------------|-------------|
| `[DISCOVERY]` | UDP broadcast discovery, node detection |
| `[ELECTION]` | LCR ring votes, forwarding, leader declaration |
| `[HEARTBEAT]` | Failure detection timing |
| `[MESSAGE]` | Chat message flow |
| `[ORDER]` | Vector clock sync, sequence assignment |
| `[RELIABILITY]` | ACK tracking, buffer flush, retransmit |
| `[VIEW]` | Group membership changes |
| `[FAULT]` | Crash detection, alive-check, ring shrink |
| `[SYNC]` | Gap recovery, missed message delivery |

---

## 13. Design Rationale

### Why These Specific Choices?

#### UDP Broadcast over Multicast/mDNS
- Matches `broadcastsender.py` example pattern exactly
- Simpler than multicast (no group management)
- Works on all LAN configurations without router support
- Zero configuration required

#### LCR over Bully Algorithm
- **Natural ring topology:** Servers form a ring sorted by binary IP (`ring.py` pattern)
- **Simplicity:** Only needs forward/drop/declare logic (`lcr-template.py`)
- **Correctness:** Highest binary IP always wins — deterministic
- **UDP-based:** Lightweight, matches the example's socket pattern

#### Binary IP Sort for Ring
- Follows `ring.py` example exactly
- Ensures consistent ordering across all nodes
- Correct numeric comparison (avoids string sort issues like `"9" > "10"`)

#### Lamport Timestamps (not Vector Clocks)
- Directly based on `lamport_clock.py` example pattern
- Single integer per node — minimal message overhead
- Total ordering is guaranteed by the leader's seq# anyway
- Vector clocks would add complexity without additional delivery benefit
- Lamport still guarantees: if A caused B then L(A) < L(B)

#### TCP for Clients, UDP for Servers
- Clients need reliable, ordered delivery → TCP
- Server-to-server is supplemented by application-level ACKs → UDP is fine
- Reduces complexity: servers don't need persistent TCP connections to each other
- Matches the simplicity of `simpleserver.py` / `simpleclient.py` examples

#### Multi-Layer Reliability (Buffer + Queue + ACK)
- Client buffer handles server failures (no message loss during reconnect)
- Server queue handles leader failures (no loss during election)
- ACK/retransmit handles UDP unreliability between servers
- Gap recovery handles any remaining edge cases on reconnect

#### Alive-Detection via Discovery Cache
- Broadcast announcements provide a natural "I'm alive" signal
- `get_alive_servers()` checks `last_seen` timestamps
- Enables one-shot dead-node removal instead of iterative timeout shrinking
- Solves the "kill multiple servers simultaneously" scenario cleanly

---

## Appendix A: Configuration Reference

| Constant | Value | Description |
|----------|-------|-------------|
| `BROADCAST_IP` | `255.255.255.255` | Subnet broadcast address |
| `BROADCAST_PORT` | `5972` | Discovery broadcast port |
| `RING_PORT` | `10001` | LCR election ring (UDP) |
| `CHAT_PORT` | `10002` | Server-to-server chat (UDP) |
| `HEARTBEAT_PORT` | `10003` | Heartbeat messages (UDP) |
| `TCP_PORT` | `10004` | Client connections (TCP) |
| `HEARTBEAT_INTERVAL` | `2.0s` | Time between heartbeats |
| `HEARTBEAT_TIMEOUT` | `30.0s` | Time before suspecting failure |
| `ELECTION_TIMEOUT` | `5.0s` | Max time for election round |
| `DISCOVERY_INTERVAL` | `3.0s` | Time between broadcasts |
| `MESSAGE_RETRY_INTERVAL` | `1.0s` | Time between retransmits |
| `MESSAGE_RETRY_MAX` | `5` | Max retransmission attempts |

## Appendix B: Message Types

| Type | Direction | Protocol | Purpose |
|------|-----------|----------|---------|
| `DISCOVERY_ANNOUNCE` | Server → Broadcast | UDP | Advertise server presence |
| `JOIN_REQUEST` | Client → Server | TCP | Join chat (includes `last_seq` for reconnect) |
| `JOIN_APPROVED` | Server → Client | TCP | Approve join, send leader/members info |
| `CHAT_MESSAGE` | Client → Server | TCP | Send a chat message (includes client VC) |
| `CHAT_ORDERED` | Leader → Servers → Clients | UDP/TCP | Sequenced message delivery |
| `CHAT_ACK` | Server → Leader | UDP | Confirm message received |
| `SYNC_RESPONSE` | Server → Client | TCP | Send missed messages on reconnect |
| `HEARTBEAT` | Leader → Followers | UDP | Liveness probe |
| `HEARTBEAT_ACK` | Follower → Leader | UDP | Liveness confirmation |
| Election `{mid, isLeader}` | Server → Neighbour | UDP | LCR ring messages |
| `VIEW_CHANGE` | Leader → All | UDP | Membership update |
| `SERVER_SHUTDOWN` | Server → All | UDP | Graceful shutdown |
| `CLIENT_DISCONNECT` | Client → Server | TCP | Graceful leave |

## Appendix C: Example Code Mapping

| Example File | How It's Used |
|--------------|--------------|
| `broadcastsender.py` | Discovery broadcast pattern — servers announce via `SO_BROADCAST` |
| `broadcastlistener.py` | Discovery listener — clients/servers listen on `0.0.0.0:5972` |
| `ring.py` | `form_ring()` for binary IP sort, `get_neighbour()` for ring traversal |
| `lcr-template.py` | Election protocol — `mid` comparison, forward/drop/declare logic |
| `simpleserver.py` | Server UDP socket pattern for heartbeats and chat |
| `simpleclient.py` | Client TCP connection pattern |
| `simplemultiserver.py` | Multi-threaded server (one thread per client) |
| `lamport_clock.py` | Lamport clock used directly for logical time ordering |

## Appendix D: Comparison with Real Systems

| Feature | Our System | WhatsApp | Discord | Tox (P2P) |
|---------|-----------|----------|---------|------------|
| Architecture | Hybrid | Client-Server | Client-Server | Pure P2P |
| Discovery | UDP Broadcast | Phone number | Manual (invite) | DHT |
| Ordering | Causal + Total | Per-chat FIFO | Server-ordered | None |
| Reliability | Buffer+Queue+ACK | Server guarantees | Server guarantees | Best-effort |
| Fault tolerance | Re-election + replication | Server redundancy | Server redundancy | Node redundancy |
| Leader election | LCR (ring) | N/A (fixed servers) | N/A (fixed servers) | N/A |
| Middleware | Custom | Proprietary | Proprietary | libsodium |
| Message during failover | Queued, delivered after | Queued on device | Server handles | May be lost |
