# DS Project Report Form — Answers

Fill these into the PDF form fields.

---

## PAGE 1: Project Information

**Group ID:** [Fill your group ID]

**Semester:** [Fill: e.g., WS25/26]

**Student Names:** [Fill your team members]

**Project Title:**
Distributed Real-Time Chat System

**Project Description:**
A distributed real-time chat system with custom middleware. Multiple chat servers form a P2P ring and elect a leader using LCR. Clients connect to any server via TCP. The leader sequences all messages (total ordering) and multicasts to other servers via UDP with ACK-based reliability. Supports dynamic discovery, automatic fault recovery, and message delivery guarantees during server failures.

**Code Repository Link:**
https://github.com/ShuvamAich/Distributed-Chat-System-Final

---

## PAGE 1: Architectural Model

**Architecture Type:** [X] Hybrid

(Clients connect to servers via TCP = client-server. Servers form a P2P ring for election and replication = peer-to-peer. Combined = hybrid.)

---

**Communication - TCP:**
Explain briefly what it is used for:
Used for client-to-server communication. Clients connect to servers via TCP (port 10004) with length-prefixed message framing. TCP provides reliable, ordered delivery for chat messages, join requests, history sync, and disconnect notifications.

**Communication - UDP:**
Explain briefly what it is used for:
Used for all server-to-server communication: discovery broadcasts (port 5972), LCR ring election votes (port 10001), heartbeats for failure detection (port 10003), and chat message replication between servers (port 10002). UDP is lightweight and sufficient since application-level ACKs handle reliability.

**Communication - Other:**
Explain briefly what it is used for:
Not applicable.

---

**Concurrency - Multithreading:**
Explain briefly what it is used for:
Each server runs ~10 threads: TCP accept loop, per-client handler threads, discovery broadcast/listen, LCR ring listener, heartbeat send/listen/check, chat replication listener, and cluster management. Workload is I/O-bound (network sockets), so threading is efficient and allows shared memory access to message history and cluster state.

**Concurrency - Multiprocessing:**
Explain briefly what it is used for:
Not used. Multiprocessing is suited for CPU-bound tasks. Our workload is I/O-bound (network communication), and threads share memory naturally (message logs, connection pools) without IPC overhead.

---

## PAGE 2: System Architecture Diagram

(Paste or draw this diagram in the architecture box)

```
+-------------+     UDP Broadcast (255.255.255.255:5972)     +-------------+
|  Client A   |<------------ Discovery ------------------>  |  Client B   |
| (Alice)     |                                              | (Bob)       |
| - Lamport   |                                              | - Lamport   |
| - SendBuffer|                                              | - SendBuffer|
+------+------+                                              +------+------+
       | TCP :10004                                                 | TCP :10004
       |                                                            |
+======|============================================================|======+
|      |              SERVER CLUSTER (LCR Ring)                      |      |
|      v                                                            v      |
| +---------+      UDP :10001       +---------+      UDP :10001  +---------+ |
| |Server .1|<--- Ring Election --->|Server .2|<-- Ring Election->|Server .3| |
| |Follower |      UDP :10002       |Follower |      UDP :10002  | LEADER  | |
| |         |<--- Chat Replicate -->|         |<-- Chat Replicate>|         | |
| |         |      UDP :10003       |         |      UDP :10003  |         | |
| |         |<--- Heartbeats ------>|         |<-- Heartbeats --->|         | |
| +---------+                       +---------+                  +---------+ |
|                                                                            |
| Each server stores: chat_history (replicated), pending_queue, client list  |
+============================================================================+

Middleware Layer (custom, no ZooKeeper/Kafka):
[Discovery] [Election/LCR] [Heartbeat] [Ordering/Lamport] [Reliability/ACK]
[Group View]

Communication:
- Client -> TCP -> Server -> UDP forward -> Leader
- Leader assigns seq# -> UDP multicast -> All Servers -> TCP -> All Clients
- Leader tracks ACKs from servers, retransmits on timeout
```

---

## PAGE 3: Dynamic Discovery of Hosts

**Discovery Mechanism:**
[X] Client discovers server
[X] Server discovers servers

(Servers broadcast their IP on UDP port 5972 every 3 seconds. Clients listen for these broadcasts to find a server. Servers also listen to discover other servers and form the ring.)

**Discovery Implemented:**
[X] Broadcast

(UDP broadcast to 255.255.255.255:5972. Based on broadcastsender.py/broadcastlistener.py example patterns.)

**Discovery Occurs:**
[X] When system starts
[X] Whenever new component comes in

(Continuous: servers broadcast every 3s. New servers are detected automatically. Membership changes trigger re-election.)

---

## PAGE 3: Voting

**Voting Implemented Using:**
[X] LeLann-Chang-Roberts Algorithm

(Servers form a ring sorted by binary IP address. Each node sends its IP clockwise. Highest IP completes full traversal and declares itself leader. Based on lcr-template.py and ring.py example code.)

**Group View Used:**
[X] Yes. Explain briefly what the group view is used for:
The group view tracks which servers are currently in the cluster and which clients are connected to each server. View changes occur when servers join, leave, or crash. It ensures all nodes agree on current membership before processing messages. Used for: determining multicast targets, heartbeat monitoring list, and announcing join/leave events.

**Nodes Identified Using:**
[X] IP addresses / IP addresses + ports

(Each server is identified by its IP address. The LCR ring is sorted by binary IP representation using socket.inet_aton(). Ports are fixed constants: 10001-10004.)

**Election Starts:**
[X] When system starts
[X] When a new server joins
[X] When the leader fails

(Election triggers: initial cluster formation, membership change detected via discovery, and heartbeat timeout detecting leader crash.)

---

## PAGE 4: Fault Tolerance

**Faults Tolerated:**
[X] Crash Faults (Leader Server, Regular Server, Client)
[X] Omission Faults

(Crash faults: servers/clients can die at any time. System detects via heartbeat timeout and re-elects. Omission faults: UDP messages can be lost. Handled by ACK/retransmit mechanism. Byzantine faults are not handled.)

**Fault Detection:**
Explain who sends heartbeats to whom, their frequency, and retries:
The leader sends UDP heartbeats to ALL follower servers every 2 seconds (port 10003). Followers respond with HEARTBEAT_ACK. If a follower doesn't ACK within 30 seconds, the leader declares it failed and removes it from the ring. Conversely, if followers don't receive a heartbeat from the leader within 30 seconds, they declare the leader failed and trigger a new LCR election. The failure is reported only once (flag prevents repeated detection).

**Recovery Strategy:**
- Leader crash: Followers detect via heartbeat timeout. Discovery cache is checked for alive nodes. Dead leader removed from ring. New LCR election among survivors. New leader reads max(seq#) from replicated chat_history to continue sequencing without gaps. Pending messages queued during election are flushed afterward.
- Server crash: Leader removes failed server from ring and heartbeat monitoring list. Clients on the dead server auto-reconnect to another server via broadcast discovery. Client send buffer holds unsent messages. On reconnect, client sends last_seq to receive only missed messages (gap recovery).
- Client crash: Server detects TCP disconnect, removes client from list, announces departure.
- Multiple simultaneous failures: Election timeout triggers get_alive_servers() which checks discovery cache for recently-broadcasting nodes. ALL dead nodes removed in one shot. Sole survivor self-elects immediately.

---

## PAGE 4: Reliable Ordered Multicast

**Type of Ordering:**
[X] Total Ordering

**Reason for Chosen Ordering:**
In a group chat, all participants must see messages in the exact same order. If Alice asks "What time?" and Bob replies "3 PM", every user must see Alice's question before Bob's answer. FIFO only orders per-sender (doesn't guarantee cross-sender ordering). Total ordering via a single sequencer (the leader assigns monotonic seq#: 1, 2, 3...) guarantees every participant delivers messages in the identical global order. Lamport timestamps provide additional logical time tracking where L(cause) < L(effect) for all causally related messages.

**Reliability Mechanism:**
[X] Acknowledgements
[X] Negative Acknowledgments
[X] Sequencing
[X] Other. Explain briefly:

The system uses multi-layer reliability:
1. ACKs: Leader tracks ACKs from each server after multicast. If no ACK within 1s, retransmits (up to 5 retries).
2. Sequencing: Monotonic seq# enables gap detection — clients/servers know if they missed a message.
3. Client send buffer: Messages typed during disconnect are queued locally and flushed after reconnecting.
4. Server pending queue: Messages arriving during leader election are queued and processed after new leader is established.
5. Gap recovery: On reconnect, client sends last_seq; server sends only missed messages (no duplicates, no loss).

**Implementation Details:**
Built in Python using only standard library (no external dependencies). Custom middleware layer with 6 modules: discovery (UDP broadcast), election (LCR ring via UDP), heartbeat (UDP failure detection), ordering (Lamport timestamps + leader sequencer), reliability (ACK/NACK/retransmit), group_view (membership tracking). Server communication uses direct UDP sockets per component. Client communication uses TCP with 4-byte length-prefixed framing. All significant events are logged with timestamps for demo visibility. System tested across multiple machines on the same LAN.
