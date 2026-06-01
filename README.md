# Distributed Real-Time Chat System

A fully custom distributed chat system with no external dependencies. Implements dynamic discovery, leader election, causal ordering, reliable multicast, and fault tolerance — all with a custom middleware layer.

## Quick Start

**Prerequisites:** Python 3.7+

**Windows Note:** Set `PYTHONIOENCODING=utf-8` for proper log display:
```cmd
set PYTHONIOENCODING=utf-8
```

### Run Tests
```bash
python test_local.py
```

### Launch Demo (single machine, multiple terminals)
```bash
python demo.py
```

### Manual Launch (multi-machine)

On each machine (same LAN):

```bash
# Machine 1
python run_server.py --id S1 --port 6001

# Machine 2
python run_server.py --id S2 --port 6002

# Machine 3
python run_server.py --id S3 --port 6003

# Any machine — client
python run_client.py --username Alice --port 8001
```

## Architecture

- **Hybrid:** Client-Server (clients to servers) + P2P (server ring)
- **Discovery:** UDP Multicast (239.1.1.1:5007)
- **Transport:** TCP with length-prefixed framing
- **Election:** LCR (LeLann-Chang-Roberts) ring algorithm
- **Ordering:** Causal (vector clocks) + Total (leader sequencer)
- **Reliability:** ACK/NACK with retransmission
- **Fault Tolerance:** Heartbeat-based failure detection + re-election

## Documentation

See [DOCUMENTATION.md](DOCUMENTATION.md) for the full technical documentation including:
- Architecture diagrams
- Algorithm explanations
- Design rationale
- Demo guide

## Project Structure

```
distributed_chat/
├── common/              # Shared utilities (message protocol, vector clocks, logger)
├── middleware/          # Custom middleware (discovery, transport, election, ordering, reliability, group view, heartbeat)
├── server/             # Chat server node
├── client/             # Chat client
├── demo.py             # Interactive demo launcher
├── test_local.py       # Integration tests
├── run_server.py       # Quick server launch
├── run_client.py       # Quick client launch
├── DOCUMENTATION.md    # Full technical documentation
└── README.md           # This file
```
