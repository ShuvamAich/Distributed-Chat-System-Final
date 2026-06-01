"""
Chat Server — a node in the distributed server cluster.
Based on simpleserver.py/simplemultiserver.py example patterns.

Uses:
- UDP broadcast for discovery (broadcastsender.py pattern)
- UDP ring for LCR election (lcr-template.py pattern)
- TCP for reliable chat message delivery (simpleserver.py pattern)
- Threading for concurrent listeners

Each server:
1. Broadcasts its presence for discovery.
2. Forms a ring with other servers (ring.py pattern).
3. Participates in LCR leader election (lcr-template.py pattern).
4. Accepts client connections via TCP.
5. Leader sequences and multicasts chat messages.
6. Monitors cluster health via UDP heartbeats.
"""

import sys
import os
import socket
import json
import struct
import threading
import time
import signal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.constants import *
from common.logger import SystemLogger
from common.network import get_local_ip, form_ring, get_neighbour
from common.lamport_clock import LamportClock
from middleware.discovery import DiscoveryService
from middleware.election import LCRElection
from middleware.heartbeat import HeartbeatService
from middleware.reliability import ReliableMulticast


class ChatServer:
    """Distributed chat server node."""

    def __init__(self, ip=None, tcp_port=None):
        self.my_ip = ip or get_local_ip()
        self.tcp_port = tcp_port or TCP_PORT
        self.logger = SystemLogger(self.my_ip, "SERVER")

        # Middleware services
        self.discovery = DiscoveryService(self.my_ip, ROLE_SERVER, self.logger)
        self.election = LCRElection(self.my_ip, self.logger,
                                    get_alive_callback=self._get_alive_members)
        self.heartbeat = HeartbeatService(
            self.my_ip, self.logger,
            self._on_server_failed, self._on_leader_failed)
        self.reliable_multicast = ReliableMulticast(
            self.my_ip, self.logger, self._udp_send_for_retransmit)

        # State
        self.lamport_clock = LamportClock()
        self.connected_clients = {}  # client_ip:port -> socket
        self.client_names = {}  # client_ip:port -> username
        self.chat_history = []  # list of ordered messages
        self.sequence_counter = 0
        self.server_members = []  # current ring members
        self._known_members = set()  # tracks what cluster_join_loop last saw
        self._pending_queue = []  # messages queued during leader election
        self._lock = threading.Lock()
        self._running = False
        self._tcp_socket = None

    def start(self):
        """Start all server services."""
        self._running = True
        self.logger.banner(f"CHAT SERVER STARTING AT {self.my_ip}")
        self.logger.system(f"TCP Port: {self.tcp_port}")
        self.logger.system(f"Ring Port (UDP): {RING_PORT}")
        self.logger.system(f"Heartbeat Port (UDP): {HEARTBEAT_PORT}")
        self.logger.system(f"Broadcast Port (UDP): {BROADCAST_PORT}")
        self.logger.system(f"PID: {os.getpid()}")
        self.logger.separator()

        # Start TCP listener for clients
        self._start_tcp_listener()

        # Start UDP chat listener (for server-to-server messages)
        self._start_chat_listener()

        # Start discovery
        self.discovery.start()

        # Start election ring listener
        self.election.start()

        # Start reliable multicast (ACK tracking + retransmit)
        self.reliable_multicast.start()

        # Start cluster formation
        threading.Thread(target=self._cluster_join_loop, daemon=True).start()

        self.logger.system("All services started — waiting for cluster formation")

    def stop(self):
        """Gracefully shut down."""
        self._running = False
        self.logger.banner("SERVER SHUTTING DOWN")

        # Notify other servers
        self._broadcast_to_servers({
            "type": MSG_SERVER_SHUTDOWN,
            "from": self.my_ip
        })

        self.heartbeat.stop()
        self.discovery.stop()
        self.election.stop()
        if self._tcp_socket:
            self._tcp_socket.close()

        # Close client connections
        with self._lock:
            for key, sock in self.connected_clients.items():
                try:
                    sock.close()
                except Exception:
                    pass
            self.connected_clients.clear()

        self.logger.system("Server stopped cleanly")

    # --- TCP Listener (for clients) ---

    def _start_tcp_listener(self):
        """Start TCP server for client connections (like simpleserver.py)."""
        self._tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._tcp_socket.bind((self.my_ip, self.tcp_port))
        self._tcp_socket.listen(10)
        self._tcp_socket.settimeout(1.0)

        t = threading.Thread(target=self._tcp_accept_loop, daemon=True)
        t.start()
        self.logger.system(f"TCP listener started on {self.my_ip}:{self.tcp_port}")

    def _tcp_accept_loop(self):
        """Accept incoming TCP connections from clients."""
        while self._running:
            try:
                conn, addr = self._tcp_socket.accept()
                client_key = f"{addr[0]}:{addr[1]}"
                self.logger.system(f"Client connected from {client_key}")
                t = threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True)
                t.start()
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle_client(self, conn, addr):
        """Handle messages from a connected client."""
        client_key = f"{addr[0]}:{addr[1]}"
        with self._lock:
            self.connected_clients[client_key] = conn

        try:
            while self._running:
                data = self._tcp_recv(conn)
                if data is None:
                    break
                msg = json.loads(data.decode())
                self._handle_client_message(msg, conn, client_key)
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            self._client_disconnected(client_key)

    def _handle_client_message(self, msg, conn, client_key):
        """Route client messages by type."""
        msg_type = msg.get("type")

        if msg_type == MSG_JOIN_REQUEST:
            username = msg.get("username", client_key)
            last_seq = msg.get("last_seq", 0)
            with self._lock:
                self.client_names[client_key] = username
            self.logger.system(
                f"Join request from {username} ({client_key}), last_seq={last_seq}")

            # Send approval
            approval = {
                "type": MSG_JOIN_APPROVED,
                "leader": self.election.get_leader(),
                "members": self.server_members,
            }
            self._tcp_send(conn, json.dumps(approval).encode())

            # Send missed messages (only those after client's last received seq)
            with self._lock:
                if last_seq > 0:
                    # Reconnecting client — send only missed messages
                    missed = [m for m in self.chat_history
                              if m.get("seq") and m["seq"] > last_seq]
                else:
                    # New client — send recent history
                    missed = self.chat_history[-50:]

            if missed:
                self.logger.sync(
                    f"Sending {len(missed)} messages to {username}",
                    f"From seq#{last_seq + 1} onward")
                sync = {"type": MSG_SYNC_RESPONSE, "messages": missed}
                self._tcp_send(conn, json.dumps(sync).encode())

            # Only announce join for new clients (not reconnects)
            if last_seq == 0:
                self._process_chat_message(
                    f"*** {username} has joined the chat ***", "SYSTEM", is_system=True)
            else:
                self._process_chat_message(
                    f"*** {username} reconnected ***", "SYSTEM", is_system=True)

        elif msg_type == MSG_CHAT_MESSAGE:
            text = msg.get("text", "")
            username = self.client_names.get(client_key, client_key)
            client_ts = msg.get("ts", 0)
            self.logger.message(
                f"Chat from {username}: \"{text}\"",
                f"Client Lamport TS={client_ts}")
            self._process_chat_message(text, username, client_ts=client_ts)

        elif msg_type == MSG_CLIENT_DISCONNECT:
            username = self.client_names.get(client_key, client_key)
            self._process_chat_message(
                f"*** {username} has left the chat ***", "SYSTEM", is_system=True)

    def _client_disconnected(self, client_key):
        """Handle client disconnect."""
        with self._lock:
            self.connected_clients.pop(client_key, None)
            username = self.client_names.pop(client_key, client_key)
        self.logger.system(f"Client disconnected: {username} ({client_key})")

    # --- UDP Chat Listener (server-to-server) ---

    def _start_chat_listener(self):
        """Listen for server-to-server chat messages via UDP."""
        t = threading.Thread(target=self._chat_listen_loop, daemon=True)
        t.start()

    def _chat_listen_loop(self):
        """Receive ordered messages from leader via UDP."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.my_ip, CHAT_PORT))
        sock.settimeout(1.0)

        while self._running:
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode())
                msg_type = msg.get("type")

                if msg_type == MSG_CHAT_ORDERED:
                    self._handle_ordered_message(msg)
                elif msg_type == MSG_SERVER_SHUTDOWN:
                    self._handle_server_shutdown(msg)
                elif msg_type == MSG_CHAT_ACK:
                    from_ip = msg.get("from")
                    ack_seq = msg.get("seq")
                    self.logger.reliability(
                        f"ACK received: Seq#{ack_seq} from {from_ip}")
                    self.reliable_multicast.handle_ack(ack_seq, from_ip)

            except socket.timeout:
                continue
            except OSError:
                break
            except Exception as e:
                if self._running:
                    self.logger.fault(f"Chat listener error: {e}")

        sock.close()

    # --- Message Processing ---

    def _process_chat_message(self, text, username, is_system=False, client_ts=0):
        """Process a new chat message — leader sequences and multicasts."""
        if self.election.is_leader():
            self._sequence_and_deliver(text, username, is_system, client_ts)

        elif self.election.get_leader():
            # Forward to leader
            leader = self.election.get_leader()
            forward_msg = {
                "type": MSG_CHAT_ORDERED,
                "text": text,
                "username": username,
                "forward_to_leader": True,
                "timestamp": time.time(),
                "system": is_system,
                "client_ts": client_ts,
            }
            self._udp_send(leader, CHAT_PORT, forward_msg)
            self.logger.message(f"Forwarded to leader {leader}")

        else:
            # No leader available (election in progress) — queue for later
            with self._lock:
                self._pending_queue.append({
                    "text": text, "username": username,
                    "is_system": is_system, "client_ts": client_ts,
                })
            self.logger.reliability(
                f"Message queued (no leader): \"{text}\" from {username}",
                f"Queue size: {len(self._pending_queue)}")

    def _sequence_and_deliver(self, text, username, is_system=False, client_ts=0):
        """Leader: assign sequence, multicast, deliver."""
        # Update Lamport clock: max(local, received) + 1
        if client_ts:
            self.lamport_clock.update(client_ts)
        else:
            self.lamport_clock.increment()
        ts = self.lamport_clock.get()

        with self._lock:
            self.sequence_counter += 1
            seq = self.sequence_counter

        ordered_msg = {
            "type": MSG_CHAT_ORDERED,
            "text": text,
            "username": username,
            "seq": seq,
            "ts": ts,
            "timestamp": time.time(),
            "system": is_system,
        }

        self.logger.order(
            f"Assigned Seq#{seq} to message",
            f"Lamport TS={ts}, From={username}")

        # Store in history (replicated log)
        with self._lock:
            self.chat_history.append(ordered_msg)

        # Reliable multicast to other servers via UDP (with ACK tracking)
        targets = [ip for ip in self.server_members if ip != self.my_ip]
        if targets:
            self.logger.reliability(
                f"MULTICAST Seq#{seq} -> {targets}",
                f"Awaiting ACKs from {len(targets)} server(s)")
            for ip in targets:
                self._udp_send(ip, CHAT_PORT, ordered_msg)
            self.reliable_multicast.track_message(seq, ordered_msg, targets)
        else:
            self.logger.reliability(f"Seq#{seq} — no other servers to multicast to")

        # Deliver to local clients
        self._deliver_to_clients(ordered_msg)

    def _flush_pending_queue(self):
        """Process all queued messages after a new leader is established."""
        with self._lock:
            queue = list(self._pending_queue)
            self._pending_queue.clear()

        if queue:
            self.logger.reliability(
                f"Flushing {len(queue)} queued messages after election")

        for item in queue:
            self._process_chat_message(
                item["text"], item["username"],
                item["is_system"], item.get("client_ts", 0))

    def _handle_ordered_message(self, msg):
        """Received sequenced message from leader — store and deliver."""
        if msg.get("forward_to_leader"):
            # This is a forwarded message — we are the leader, process it
            text = msg.get("text", "")
            username = msg.get("username", "unknown")
            is_system = msg.get("system", False)
            client_ts = msg.get("client_ts", 0)
            self._process_chat_message(text, username, is_system, client_ts=client_ts)
            return

        seq = msg.get("seq")
        self.logger.order(
            f"Received ordered message Seq#{seq}",
            f"From={msg.get('username')}, VC={msg.get('vc')}")

        # Store in history
        with self._lock:
            self.chat_history.append(msg)

        # Deliver to local clients
        self._deliver_to_clients(msg)

        # Send ACK to leader
        leader = self.election.get_leader()
        if leader:
            ack = {"type": MSG_CHAT_ACK, "from": self.my_ip, "seq": seq}
            self._udp_send(leader, CHAT_PORT, ack)
            self.logger.reliability(f"ACK sent for Seq#{seq} to leader {leader}")

    def _deliver_to_clients(self, msg):
        """Deliver an ordered message to all locally connected clients via TCP."""
        with self._lock:
            clients = dict(self.connected_clients)

        delivery = json.dumps({
            "type": MSG_CHAT_ORDERED,
            "text": msg.get("text", ""),
            "username": msg.get("username", ""),
            "seq": msg.get("seq"),
            "ts": msg.get("ts"),
            "timestamp": msg.get("timestamp"),
            "system": msg.get("system", False),
        }).encode()

        for key, sock in clients.items():
            try:
                self._tcp_send(sock, delivery)
            except Exception:
                pass

    # --- Cluster Management ---

    def _cluster_join_loop(self):
        """Periodically discover servers and form the ring."""
        time.sleep(2)
        last_count = 0
        stable = 0

        while self._running:
            servers = self.discovery.get_servers()
            all_ips = sorted(set(list(servers.keys()) + [self.my_ip]))

            if len(all_ips) == last_count:
                stable += 1
            else:
                stable = 0
            last_count = len(all_ips)

            # Trigger election when membership changes and is stable
            if stable >= 1 and set(all_ips) != self._known_members:
                self.logger.system(
                    f"Membership changed: {sorted(self._known_members)} -> {all_ips}")

                self.server_members = all_ips
                self._known_members = set(all_ips)
                self.election.update_ring(all_ips)
                self.election.start_election(
                    reason=f"membership change ({len(all_ips)} servers)")

                leader = self.election.wait_for_leader(timeout=ELECTION_TIMEOUT + 5)
                if leader:
                    # Stop old heartbeat and restart with new configuration
                    self.heartbeat.stop()
                    self._post_election_setup()
                else:
                    self.logger.fault("Election timed out, will retry next cycle")
                    # Reset so next cycle triggers again
                    self._known_members = set()

            time.sleep(DISCOVERY_INTERVAL)

    def _post_election_setup(self):
        """Configure heartbeats after election and flush queued messages."""
        leader = self.election.get_leader()
        is_leader = self.election.is_leader()

        if is_leader:
            # CRITICAL: Continue sequence from where old leader left off
            # to maintain total ordering continuity across leader changes
            with self._lock:
                max_seq = 0
                for msg in self.chat_history:
                    seq = msg.get("seq", 0)
                    if isinstance(seq, int) and seq > max_seq:
                        max_seq = seq
                if max_seq > self.sequence_counter:
                    self.logger.order(
                        f"Continuing sequence from old leader",
                        f"Old counter={self.sequence_counter}, "
                        f"Max seq in history={max_seq}")
                    self.sequence_counter = max_seq

            other_ips = [ip for ip in self.server_members if ip != self.my_ip]
            self.heartbeat.start(is_leader=True, monitored_ips=other_ips)
        else:
            self.heartbeat.start(is_leader=False, leader_ip=leader)

        self.logger.separator()
        self.logger.system(
            f"Post-election setup complete",
            f"Leader={leader}, IsLeader={is_leader}")

        # Flush messages that were queued during election
        self._flush_pending_queue()

    # --- Fault Tolerance ---

    def _on_server_failed(self, failed_ip):
        """Leader detected a server failure."""
        self.logger.fault(f"Server {failed_ip} FAILED — removing from cluster")
        self.server_members = [ip for ip in self.server_members if ip != failed_ip]
        self._known_members = set(self.server_members)
        self.discovery.remove_node(failed_ip)
        self.election.update_ring(self.server_members)
        other_ips = [ip for ip in self.server_members if ip != self.my_ip]
        self.heartbeat.update_monitored_nodes(other_ips)

    def _on_leader_failed(self):
        """Follower detected leader failure — trigger re-election."""
        old_leader = self.election.get_leader()
        self.logger.fault(f"Leader {old_leader} unreachable — re-electing")

        self.heartbeat.stop()

        # Check who is actually still alive (broadcasting recently)
        self.discovery.remove_node(old_leader)
        alive_servers = self.discovery.get_alive_servers()
        alive_ips = sorted(set(list(alive_servers.keys()) + [self.my_ip]))
        # Ensure dead leader is excluded
        alive_ips = [ip for ip in alive_ips if ip != old_leader]

        self.logger.fault(
            f"Alive nodes after leader failure: {alive_ips}")

        self.server_members = alive_ips
        self._known_members = set(alive_ips)
        self.election.update_ring(alive_ips)

        time.sleep(1)
        self.election.start_election(reason="leader failure")

        leader = self.election.wait_for_leader(timeout=ELECTION_TIMEOUT + 2)
        if leader:
            self._post_election_setup()

    def _handle_server_shutdown(self, msg):
        """A server announced graceful shutdown."""
        from_ip = msg.get("from")
        self.logger.system(f"Server {from_ip} shutting down")
        if self.election.is_leader():
            self.server_members = [ip for ip in self.server_members if ip != from_ip]
            self.election.update_ring(self.server_members)
            other_ips = [ip for ip in self.server_members if ip != self.my_ip]
            self.heartbeat.update_monitored_nodes(other_ips)

    def _get_alive_members(self):
        """Return list of server IPs that are still alive (recently broadcasting).
        Used by election timeout to rebuild ring with only reachable nodes."""
        alive_servers = self.discovery.get_alive_servers()
        alive_ips = sorted(set(list(alive_servers.keys()) + [self.my_ip]))
        self.server_members = alive_ips
        self._known_members = set(alive_ips)
        return alive_ips

    def _udp_send_for_retransmit(self, target_ip, msg_dict):
        """Callback for ReliableMulticast retransmissions."""
        self._udp_send(target_ip, CHAT_PORT, msg_dict)

    # --- Network Helpers ---

    def _udp_send(self, target_ip, port, msg_dict):
        """Send a JSON message via UDP (like simpleclient.py pattern)."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(json.dumps(msg_dict).encode(), (target_ip, port))
            sock.close()
        except Exception as e:
            self.logger.fault(f"UDP send to {target_ip}:{port} failed: {e}")

    def _multicast_to_servers(self, msg_dict):
        """Send to all servers except self via UDP."""
        for ip in self.server_members:
            if ip != self.my_ip:
                self._udp_send(ip, CHAT_PORT, msg_dict)

    def _broadcast_to_servers(self, msg_dict):
        """Send to all servers via UDP."""
        for ip in self.server_members:
            if ip != self.my_ip:
                self._udp_send(ip, CHAT_PORT, msg_dict)

    @staticmethod
    def _tcp_send(sock, data):
        """Send length-prefixed data over TCP."""
        length = struct.pack("!I", len(data))
        sock.sendall(length + data)

    @staticmethod
    def _tcp_recv(sock):
        """Receive length-prefixed data over TCP."""
        header = ChatServer._recv_exact(sock, 4)
        if header is None:
            return None
        length = struct.unpack("!I", header)[0]
        return ChatServer._recv_exact(sock, length)

    @staticmethod
    def _recv_exact(sock, n):
        """Receive exactly n bytes."""
        data = b""
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    # --- Status ---

    def get_status(self):
        return {
            "ip": self.my_ip,
            "leader": self.election.get_leader(),
            "is_leader": self.election.is_leader(),
            "ring": self.election.ring,
            "members": self.server_members,
            "clients": list(self.client_names.values()),
            "history_size": len(self.chat_history),
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Distributed Chat Server")
    parser.add_argument("--ip", type=str, default=None,
                        help="Server IP address (default: auto-detect)")
    parser.add_argument("--port", type=int, default=TCP_PORT,
                        help=f"TCP port for clients (default: {TCP_PORT})")
    args = parser.parse_args()

    server = ChatServer(ip=args.ip, tcp_port=args.port)

    def shutdown(sig, frame):
        print("\n")
        server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    server.start()

    print(f"\n{'─'*60}")
    print(f"  Server running. Commands:")
    print(f"    status  - Show server status")
    print(f"    quit    - Shutdown server")
    print(f"{'─'*60}\n")

    while True:
        try:
            cmd = input().strip().lower()
            if cmd == "status":
                status = server.get_status()
                print(f"\n{'='*40}")
                print(f"  Server Status")
                print(f"{'='*40}")
                print(f"  IP:         {status['ip']}")
                print(f"  Leader:     {status['leader']} {'(ME)' if status['is_leader'] else ''}")
                print(f"  Ring:       {' -> '.join(status['ring'])}")
                print(f"  Members:    {status['members']}")
                print(f"  Clients:    {status['clients']}")
                print(f"  History:    {status['history_size']} messages")
                print(f"{'='*40}\n")
            elif cmd in ("quit", "exit", "q"):
                server.stop()
                break
        except (EOFError, KeyboardInterrupt):
            server.stop()
            break


if __name__ == "__main__":
    main()
