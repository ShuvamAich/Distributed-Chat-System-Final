"""
System-wide constants for the distributed chat system (AP design).
"""

# Network Configuration
BROADCAST_IP = "255.255.255.255"  # Subnet broadcast (works on most LANs)
BROADCAST_PORT = 5972             # Discovery broadcast port
RING_PORT = 7001                  # LCR election ring port (UDP) — control plane only
CHAT_PORT = 7002                  # Peer-to-peer chat + anti-entropy port (UDP)
HEARTBEAT_PORT = 7003             # Heartbeat port (UDP) — control plane only
TCP_PORT = 7004                   # TCP port for client <-> server delivery

# Buffer size for UDP
BUFFER_SIZE = 65507               # Max UDP payload; anti-entropy batches can be large

# Timing (seconds)
HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0            # ~3x interval — responsive crash detection for the demo
ELECTION_TIMEOUT = 5.0
DISCOVERY_INTERVAL = 3.0
RECONNECT_INTERVAL = 5.0
GAP_CHECK_INTERVAL = 4.0          # Anti-entropy reconciliation period
HOLDBACK_TIMEOUT = 4.0            # Force-release a causally-held message after this long
REORDER_WINDOW = 1.5             # Client buffers deliverable msgs this long before printing

# Protocol Message Types
# -- Discovery (UDP broadcast) --
MSG_DISCOVERY_ANNOUNCE = "DISCOVERY_ANNOUNCE"
MSG_DISCOVERY_REQUEST = "DISCOVERY_REQUEST"
MSG_DISCOVERY_RESPONSE = "DISCOVERY_RESPONSE"
# -- Client <-> server (TCP) --
MSG_JOIN_REQUEST = "JOIN_REQUEST"
MSG_JOIN_APPROVED = "JOIN_APPROVED"
MSG_CHAT_MESSAGE = "CHAT_MESSAGE"     # client -> server (raw text)
MSG_CHAT_DELIVER = "CHAT_DELIVER"     # server -> client (causally delivered message)
MSG_SYNC_RESPONSE = "SYNC_RESPONSE"   # server -> client (missed-message catch-up)
MSG_CLIENT_DISCONNECT = "CLIENT_DISCONNECT"
# -- Server <-> server (UDP, peer-to-peer mesh) --
MSG_CHAT_BCAST = "CHAT_BCAST"         # peer-to-peer chat broadcast (no leader hop)
MSG_GAP_SUMMARY = "GAP_SUMMARY"       # anti-entropy: "here is what I hold"
MSG_GAP_REQUEST = "GAP_REQUEST"       # anti-entropy: "please send me these ids"
MSG_GAP_RESPONSE = "GAP_RESPONSE"     # anti-entropy: "here are the messages you asked for"
MSG_SERVER_SHUTDOWN = "SERVER_SHUTDOWN"
# -- Control plane (UDP) --
MSG_HEARTBEAT = "HEARTBEAT"
MSG_HEARTBEAT_ACK = "HEARTBEAT_ACK"
MSG_ELECTION = "ELECTION"
MSG_ELECTED = "ELECTED"

# Node Roles
ROLE_SERVER = "SERVER"
ROLE_CLIENT = "CLIENT"
