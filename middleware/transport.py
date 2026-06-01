"""
Transport Layer — now using direct UDP and TCP sockets
(following the example code patterns from simpleclient.py/simpleserver.py).

UDP: Used for discovery (broadcast), election (ring), heartbeats, server-to-server chat.
TCP: Used for client-to-server communication (reliable, ordered).

No abstraction layer needed — each component creates its own sockets
as shown in the example code.
"""

# This module is kept for compatibility but the transport logic
# is now embedded directly in each component following the example patterns.
# See: chat_server.py (_udp_send, _tcp_send, etc.)
#      chat_client.py (_tcp_send, _tcp_recv, etc.)
#      election.py (_send_to_neighbour)
#      heartbeat.py (_listen_loop, _send_heartbeats_loop)
#      discovery.py (_broadcast_loop, _listen_loop)
