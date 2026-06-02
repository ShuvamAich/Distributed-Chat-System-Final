# Distributed Real-Time Chat System

A fully custom distributed chat system with no external dependencies. Implements dynamic discovery, leader election, Lamport clock ordering, reliable multicast, and fault tolerance — all with a custom middleware layer.

## Quick Start

**Prerequisites:** Python 3.7+ (no pip install needed)

**Windows:** Set encoding for proper log display:
```cmd
set PYTHONIOENCODING=utf-8
```

### Run Tests
```bash
python test_local.py
```

### Multi-Machine Demo (recommended)

On each machine (same LAN):

```bash
# Machine 1 — Server
python run_server.py

# Machine 2 — Server
python run_server.py

# Machine 3 — Server
python run_server.py

# Any machine — Client
python run_client.py --username Alice

# Any machine — Client
python run_client.py --username Bob
```

Servers auto-discover each other via UDP broadcast. Clients auto-discover servers. No IPs needed — everything is automatic on a LAN, provided UDP/TCP traffic on the chat ports is allowed by the OS firewall.

For a single local server on the same machine, client auto-discovery also works. For multi-server single-machine demos, use the explicit loopback IP setup below.

If two different Windows machines do not discover each other, allow inbound UDP on ports 5972, 7001, 7002, 7003 and inbound TCP on port 7004 on both machines, and make sure both are on the same IPv4 subnet.

### Single-Machine Demo (loopback)

```bash
# Terminal 1
python run_server.py --ip 127.0.0.1

# Terminal 2
python run_server.py --ip 127.0.0.2

# Terminal 3
python run_server.py --ip 127.0.0.3

# Terminal 4
python run_client.py --username Alice --server 127.0.0.1

# Terminal 5
python run_client.py --username Bob --server 127.0.0.2
```

### Server Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--ip` | Server IP to bind to | Auto-detect LAN IP |
| `--port` | TCP port for client connections | 10004 |

### Client Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--username` / `-u` | Chat display name | User-{PID} |
| `--server` / `-s` | Server IP (skip auto-discovery) | Auto-discover via broadcast |

### Commands

**Server:** `status` (show cluster info), `quit` (shutdown)

**Client:** `/status`, `/history`, `/quit`

## Architecture

- **Hybrid:** Client-Server (TCP) + P2P Server Ring (UDP)
- **Discovery:** UDP Broadcast (255.255.255.255:5972)
- **Election:** LCR ring algorithm via UDP (port 10001)
- **Ordering:** Lamport Timestamps + Total Ordering (leader sequencer)
- **Reliability:** ACK/NACK with retransmission + client send buffer + server pending queue
- **Fault Tolerance:** Heartbeat failure detection (30s timeout) + automatic re-election + gap recovery on reconnect

## Key Features

- **Zero configuration** — servers find each other automatically via broadcast
- **No message loss** — client buffer + server queue + gap recovery guarantee delivery
- **Leader failover** — automatic re-election within seconds, no manual intervention
- **Implicit heartbeats** — active message traffic counts as liveness proof
- **Demo-friendly** — rich colored terminal logs showing every system event with timestamps

## Documentation

See [DOCUMENTATION.md](DOCUMENTATION.md) for full technical documentation:
- System architecture diagrams
- LCR election algorithm walkthrough
- Lamport clock synchronization
- Reliability mechanisms (4 layers)
- Fault tolerance scenarios
- Design rationale

## Project Structure

```
distributed_chat/
├── common/              # Lamport clock, network utils (form_ring, get_neighbour), logger
├── middleware/          # Discovery, election (LCR), heartbeat, ordering, reliability, group view
├── server/             # Chat server node
├── client/             # Chat client
├── Example Code/       # Reference patterns (broadcastsender, lcr-template, ring, lamport_clock, etc.)
├── demo.py             # Interactive demo launcher
├── test_local.py       # Unit tests (5 tests)
├── run_server.py       # Quick server launch
├── run_client.py       # Quick client launch
├── DOCUMENTATION.md    # Full technical documentation
├── FORM_ANSWERS.md     # Project report form answers
└── README.md           # This file
```
