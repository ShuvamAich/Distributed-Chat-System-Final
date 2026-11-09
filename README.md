# Distributed Real-Time Chat System (AP)

A fully custom distributed group chat with no external dependencies (no Kafka, no
ZooKeeper). It is **availability-first (AP)**: you can always chat with the people
your side of the network can reach, even while a server — or the elected
coordinator — is down or being re-elected. Ordering is **causal, converging to an
eventual total order**, enforced with vector clocks and a Lamport-timestamp
tiebreaker. Everything runs in plain Python over a custom middleware layer.

Where it sits on CAP: we prioritise **Availability + Partition-tolerance** and
deliberately avoid linearizability (which would force the chat to pause during a
partition). See [proposal.md](proposal.md) for the full design rationale.

## Quick Start

**Prerequisites:** Python 3.7+ (no `pip install` needed).

**Windows:** `set PYTHONIOENCODING=utf-8` for proper log display.

### Run the logic tests
```bash
python test_local.py
```
Deterministic, network-free tests of the ordering guarantees (causal hold-back,
force-release liveness, anti-entropy convergence, concurrent-message tiebreaker,
partition-merge convergence).

### Two-Machine Demo (recommended — both machines act as servers)

On two machines on the same LAN subnet:
```bash
# Machine 1
python run_server.py
python run_client.py --username Alice

# Machine 2
python run_server.py
python run_client.py --username Bob
```
Servers auto-discover each other via UDP broadcast; clients auto-discover a
server. Nothing is hardcoded — any number of servers and clients works. (If two
machines don't find each other, allow inbound UDP on 5972/7001/7002/7003 and TCP
on 7004, and confirm both are on the same IPv4 subnet.)

### Single-Machine Smoke Test (one server + clients)
```bash
# Terminal 1
python run_server.py --ip 127.0.0.1
# Terminal 2
python run_client.py --username Alice --server 127.0.0.1
# Terminal 3
python run_client.py --username Bob   --server 127.0.0.1
```
The full multi-server mesh is best shown across two real machines, because
discovery and the server mesh share fixed UDP ports.

### Arguments

| Server | Description | Default |
|--------|-------------|---------|
| `--ip` | IP to bind to | Auto-detect LAN IP |
| `--port` | TCP port for clients | 7004 |

| Client | Description | Default |
|--------|-------------|---------|
| `--username` / `-u` | Display name | `User-{PID}` |
| `--server` / `-s` | Server IP (skip discovery) | Auto-discover |
| `--port` / `-p` | Server TCP port | 7004 |

### Commands

**Server:** `status` (cluster + causal-delivery state) · `history` (converged total
order) · `partition <ip>` (demo: drop all traffic to/from a peer) · `heal` (restore)
· `quit`

**Client:** `/status` · `/history` (converged order) · `/quit`

## Architecture

- **Hybrid:** client–server over TCP + a peer-to-peer **server mesh** over UDP.
- **Discovery:** UDP broadcast (255.255.255.255:5972), zero-config, age-out on silence.
- **Voting:** LCR ring election (UDP :7001) — **control plane only**. Highest IP wins.
- **Coordinator:** the elected leader does **only** heartbeat liveness + membership.
  It is deliberately **off the message path** — it does not route, order, or relay chat.
- **Messaging:** every server accepts a client message, shows it to its own users
  immediately, and gossips it peer-to-peer to the mesh (UDP :7002). No leader hop.
- **Ordering:** vector clocks give **causal delivery** (a reply is never shown before
  the message it answers); a `(lamport, server-id, counter)` tiebreaker gives an
  **eventual total order** identical on every replica. A causally-held message is
  force-released after a timeout so the system never blocks.
- **Reliability:** decentralized **anti-entropy** gap-fill — servers periodically
  exchange a compact digest and pull any messages they're missing. No central
  ACK/retransmit authority. Duplicates are ignored by globally-unique message id.
- **Fault tolerance:** heartbeat crash detection (~6 s); chat keeps flowing through
  server crashes, leader crashes, and network partitions. On partition heal, anti-
  entropy reconciles and all replicas re-converge.

## Message identity & clocks

- **Message id:** `"<ip>:<epoch>:<counter>"` — globally unique with no coordinator.
  The boot `epoch` makes ids restart-safe (a restarted server never reuses an id).
- **Node incarnation id:** `"<ip>:<epoch>"` — the stable key for vector clocks.
- **Vector clock:** per-incarnation counters — gate live causal delivery.
- **Lamport timestamp:** scalar tiebreaker — defines the eventual total order.

## Key Properties

- **Zero configuration** — servers and clients find each other via broadcast.
- **Always available** — chat never pauses for an election, a leader, or a quorum.
- **Causal + convergent** — causal live order; identical converged order everywhere.
- **Partition-tolerant** — both sides keep chatting during a split and merge on heal.
- **Scalable** — no hardcoded server count or user list.
- **Demo-friendly** — rich colored logs for every event; `history`/`status` commands.

## Documentation

- [proposal.md](proposal.md) — design rationale and CAP positioning.
- [DOCUMENTATION.md](DOCUMENTATION.md) — full technical documentation.
- [FORM_ANSWERS.md](FORM_ANSWERS.md) — project report form answers.

## Project Structure

```
distributed_chat/
├── common/         # constants, vector_clock, lamport_clock, message envelope, network, logger
├── middleware/     # discovery, election (LCR), heartbeat, ordering (causal delivery), antientropy, group_view
├── server/         # chat server node (peer-to-peer mesh, no leader on message path)
├── client/         # chat client (thin: sends text, renders causal order)
├── Example Code/   # course reference patterns (broadcast, lcr-template, ring, lamport_clock, ...)
├── demo.py         # interactive demo launcher + instructions
├── test_local.py   # AP engine logic tests
├── run_server.py   # server launch
├── run_client.py   # client launch
├── proposal.md     # design proposal (target design)
├── DOCUMENTATION.md
├── FORM_ANSWERS.md
└── README.md
```
