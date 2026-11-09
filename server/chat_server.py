"""
Chat Server — a node in the distributed server cluster (AP design).

Availability-first. There is NO leader on the message path and NO global
sequence number. Every server:

1. Broadcasts its presence for discovery (UDP broadcast).
2. Forms an LCR ring for leader election — CONTROL PLANE ONLY.
3. Accepts client connections over TCP.
4. On a client message: stamps it with a vector clock + Lamport timestamp,
   delivers it to its own clients immediately, and gossips it peer-to-peer to
   every other server (full mesh) — no leader hop.
5. Delivers incoming peer messages in causal order (vector clocks), converging
   to an eventual total order sorted by (lamport, origin, counter).
6. Fills gaps with decentralized anti-entropy (no central ACK/retransmit).

The elected leader does only control-plane work: heartbeat liveness + the
authoritative membership list. If it is down or being re-elected, chat keeps
flowing — which is the whole point of keeping it off the message path.
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
from common.network import get_local_ip
from middleware.discovery import DiscoveryService
from middleware.election import LCRElection
from middleware.heartbeat import HeartbeatService
from middleware.group_view import GroupView
from middleware.ordering import CausalDelivery
from middleware.antientropy import AntiEntropy


class ChatServer:
    """Distributed chat server node (availability-first)."""

    def __init__(self, ip=None, tcp_port=None):
        self.my_ip = ip or get_local_ip()
        self.tcp_port = tcp_port or TCP_PORT
        self.epoch = int(time.time())
        self.node_id = f"{self.my_ip}:{self.epoch}"   # stable per-incarnation id
        self.logger = SystemLogger(self.my_ip, "SERVER")

        # Demo-only simulated partition: IPs whose packets we drop in both directions.
        self._blocked_peers = set()

        # Middleware services
        self.discovery = DiscoveryService(self.my_ip, ROLE_SERVER, self.logger,
                                          tcp_port=self.tcp_port,
                                          block_filter=self.is_blocked)
        self.election = LCRElection(self.my_ip, self.logger,
                                    get_alive_callback=self._get_alive_members,
                                    block_filter=self.is_blocked)
        self.heartbeat = HeartbeatService(
            self.my_ip, self.logger,
            self._on_server_failed, self._on_leader_failed,
            block_filter=self.is_blocked)
        self.group_view = GroupView(self.my_ip, self.logger)

        # Causal-delivery engine (replaces the leader sequencer)
        self.delivery = CausalDelivery(self.node_id, self.logger,
                                       HOLDBACK_TIMEOUT, time.time)
        # Decentralized gap-fill (replaces leader ACK/retransmit)
        self.antientropy = AntiEntropy(self.my_ip, self.logger, self.delivery,
                                       self._udp_chat_send, self._alive_peers,
                                       GAP_CHECK_INTERVAL)

        # State
        self.connected_clients = {}   # client_key -> socket
        self.client_names = {}        # client_key -> username
        self.server_members = []      # current known server IPs
        self._known_members = set()   # what the cluster-join loop last acted on
        self._lock = threading.Lock()
        self._running = False
        self._tcp_socket = None

    # --- Lifecycle ---

    def start(self):
        self._running = True
        self.logger.banner(f"CHAT SERVER STARTING AT {self.my_ip} (epoch {self.epoch})")
        self.logger.system(f"Node ID: {self.node_id}")
        self.logger.system(f"TCP Port: {self.tcp_port}  Chat/UDP: {CHAT_PORT}")
        self.logger.system("Mode: AP (causal consistency, leader OFF message path)")
        self.logger.separator()

        self._start_tcp_listener()
        self._start_chat_listener()
        self.discovery.start()
        self.election.start()
        self.antientropy.start()

        threading.Thread(target=self._cluster_join_loop, daemon=True).start()
        threading.Thread(target=self._force_release_loop, daemon=True).start()

        self.logger.system("All services started — waiting for cluster formation")

    def stop(self):
        self._running = False
        self.logger.banner("SERVER SHUTTING DOWN")
        self._broadcast_to_servers({"type": MSG_SERVER_SHUTDOWN, "from": self.my_ip})
        self.heartbeat.stop()
        self.discovery.stop()
        self.election.stop()
        self.antientropy.stop()
        if self._tcp_socket:
            self._tcp_socket.close()
        with self._lock:
            for sock in self.connected_clients.values():
                try:
                    sock.close()
                except Exception:
                    pass
            self.connected_clients.clear()
        self.logger.system("Server stopped cleanly")

    # --- TCP Listener (clients) ---

    def _start_tcp_listener(self):
        self._tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._tcp_socket.bind(("0.0.0.0", self.tcp_port))
        self._tcp_socket.listen(10)
        self._tcp_socket.settimeout(1.0)
        threading.Thread(target=self._tcp_accept_loop, daemon=True).start()
        self.logger.system(f"TCP listener started on {self.my_ip}:{self.tcp_port}")

    def _tcp_accept_loop(self):
        while self._running:
            try:
                conn, addr = self._tcp_socket.accept()
                client_key = f"{addr[0]}:{addr[1]}"
                self.logger.system(f"Client connected from {client_key}")
                threading.Thread(target=self._handle_client,
                                 args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle_client(self, conn, addr):
        client_key = f"{addr[0]}:{addr[1]}"
        with self._lock:
            self.connected_clients[client_key] = conn
        try:
            while self._running:
                data = self._tcp_recv(conn)
                if data is None:
                    break
                self._handle_client_message(json.loads(data.decode()), conn, client_key)
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            self._client_disconnected(client_key)

    def _handle_client_message(self, msg, conn, client_key):
        msg_type = msg.get("type")

        if msg_type == MSG_JOIN_REQUEST:
            username = msg.get("username", client_key)
            marker = msg.get("marker", {})   # client's last-seen per-origin counters
            with self._lock:
                self.client_names[client_key] = username
            self.group_view.add_client(client_key, username)
            self.logger.system(f"Join request from {username} ({client_key})")

            self._tcp_send(conn, json.dumps({
                "type": MSG_JOIN_APPROVED,
                "leader": self.election.get_leader(),
                "members": self.server_members,
            }).encode())

            # Catch the client up on whatever it is missing (works across reconnects
            # to a different server — causal context preserved via the marker).
            missed = self.delivery.missed_for_client(marker)
            if missed:
                self.logger.sync(f"Catching up {username} with {len(missed)} message(s)")
                self._tcp_send(conn, json.dumps({
                    "type": MSG_SYNC_RESPONSE,
                    "messages": [self._client_view(e) for e in missed],
                }).encode())

            notice = ("joined" if not marker else "reconnected")
            self._originate(f"*** {username} has {notice} the chat ***",
                            "SYSTEM", system=True)

        elif msg_type == MSG_CHAT_MESSAGE:
            text = msg.get("text", "")
            username = self.client_names.get(client_key, client_key)
            self.logger.message(f"Chat from {username}: \"{text}\"")
            self._originate(text, username)

        elif msg_type == MSG_CLIENT_DISCONNECT:
            username = self.client_names.get(client_key, client_key)
            self._originate(f"*** {username} has left the chat ***",
                            "SYSTEM", system=True)

    def _client_disconnected(self, client_key):
        with self._lock:
            self.connected_clients.pop(client_key, None)
            username = self.client_names.pop(client_key, client_key)
        self.group_view.remove_client(client_key)
        self.logger.system(f"Client disconnected: {username} ({client_key})")

    # --- Server-to-server UDP listener (chat mesh + anti-entropy) ---

    def _start_chat_listener(self):
        threading.Thread(target=self._chat_listen_loop, daemon=True).start()

    def _chat_listen_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", CHAT_PORT))
        sock.settimeout(1.0)

        while self._running:
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
                if self.is_blocked(addr[0]):
                    continue  # simulated partition: drop peer traffic
                msg = json.loads(data.decode())
                msg_type = msg.get("type")

                # Any peer message is implicit proof of liveness.
                self.heartbeat.mark_alive(addr[0])

                if msg_type == MSG_CHAT_BCAST:
                    newly = self.delivery.on_receive(msg["env"])
                    self._deliver_to_clients(newly)
                elif msg_type == MSG_GAP_SUMMARY:
                    self.antientropy.handle_summary(msg["from"], msg["digest"])
                elif msg_type == MSG_GAP_RESPONSE:
                    newly = self.antientropy.handle_response(msg["messages"])
                    self._deliver_to_clients(newly)
                elif msg_type == MSG_SERVER_SHUTDOWN:
                    self._handle_server_shutdown(msg)

            except socket.timeout:
                continue
            except OSError:
                break
            except Exception as e:
                if self._running:
                    self.logger.fault(f"Chat listener error: {e}")
        sock.close()

    # --- Message path (NO leader hop) ---

    def _originate(self, text, username, system=False):
        """Accept a local client's message: stamp, show locally, gossip to peers."""
        env = self.delivery.originate(username, text, system=system)
        self._deliver_to_clients([env])
        self._broadcast_chat(env)

    def _broadcast_chat(self, env):
        """Best-effort peer-to-peer multicast across the full server mesh."""
        peers = [ip for ip in self.server_members if ip != self.my_ip]
        for ip in peers:
            self._udp_chat_send(ip, {"type": MSG_CHAT_BCAST, "env": env})
        if peers:
            self.logger.message(f"Gossiped {env['msg_id']} -> {peers}")

    def _force_release_loop(self):
        """Periodically release causally-held messages whose deps never arrived."""
        while self._running:
            time.sleep(1.0)
            released = self.delivery.force_release_due()
            if released:
                self._deliver_to_clients(released)

    def _deliver_to_clients(self, envelopes):
        """Push causally-delivered messages to all locally-connected clients via TCP."""
        if not envelopes:
            return
        with self._lock:
            clients = dict(self.connected_clients)
        for env in envelopes:
            payload = json.dumps({
                "type": MSG_CHAT_DELIVER,
                **self._client_view(env),
            }).encode()
            for sock in clients.values():
                try:
                    self._tcp_send(sock, payload)
                except Exception:
                    pass

    @staticmethod
    def _client_view(env):
        """Fields a client needs to display + maintain its last-seen marker."""
        return {
            "text": env["text"],
            "username": env["username"],
            "system": env["system"],
            "origin_id": env["origin_id"],
            "origin_counter": env["origin_counter"],
            "lamport_ts": env["lamport_ts"],
            "vector_clock": env["vector_clock"],
        }

    # --- Cluster management (control plane; never gates chat) ---

    def _cluster_join_loop(self):
        time.sleep(2)
        last_count, stable = 0, 0
        while self._running:
            servers = self.discovery.get_alive_servers()
            all_ips = sorted(set(list(servers.keys()) + [self.my_ip]))

            stable = stable + 1 if len(all_ips) == last_count else 0
            last_count = len(all_ips)

            if stable >= 1 and set(all_ips) != self._known_members:
                self.logger.system(
                    f"Membership changed: {sorted(self._known_members)} -> {all_ips}")
                self.server_members = all_ips
                self._known_members = set(all_ips)
                self.group_view.install_view(all_ips, reason="membership change")
                self.election.update_ring(all_ips)
                self.election.start_election(
                    reason=f"membership change ({len(all_ips)} servers)")
                # Control-plane wait only — chat threads are unaffected meanwhile.
                if self.election.wait_for_leader(timeout=ELECTION_TIMEOUT + 2):
                    self.heartbeat.stop()
                    self._configure_heartbeat()
                else:
                    self.logger.fault("Election timed out, will retry next cycle")
                    self._known_members = set()

            time.sleep(DISCOVERY_INTERVAL)

    def _configure_heartbeat(self):
        leader = self.election.get_leader()
        if self.election.is_leader():
            others = [ip for ip in self.server_members if ip != self.my_ip]
            self.heartbeat.start(is_leader=True, monitored_ips=others)
        else:
            self.heartbeat.start(is_leader=False, leader_ip=leader)
        self.logger.separator()
        self.logger.system("Control plane ready",
                           f"Leader={leader}, IsLeader={self.election.is_leader()}")

    # --- Fault tolerance (control plane only) ---

    def _on_server_failed(self, failed_ip):
        self.logger.fault(f"Server {failed_ip} FAILED — removing from cluster")
        self.server_members = [ip for ip in self.server_members if ip != failed_ip]
        self._known_members = set(self.server_members)
        self.discovery.remove_node(failed_ip)
        self.group_view.install_view(self.server_members, reason=f"{failed_ip} crashed")
        self.election.update_ring(self.server_members)
        self.heartbeat.update_monitored_nodes(
            [ip for ip in self.server_members if ip != self.my_ip])

    def _on_leader_failed(self):
        old_leader = self.election.get_leader()
        self.logger.fault(f"Leader {old_leader} unreachable — re-electing "
                          f"(chat keeps flowing)")
        self.heartbeat.stop()
        self.discovery.remove_node(old_leader)
        alive = sorted(set(list(self.discovery.get_alive_servers().keys()) + [self.my_ip]))
        alive = [ip for ip in alive if ip != old_leader]
        self.server_members = alive
        self._known_members = set(alive)
        self.group_view.install_view(alive, reason=f"leader {old_leader} failed")
        self.election.update_ring(alive)
        time.sleep(1)
        self.election.start_election(reason="leader failure")
        if self.election.wait_for_leader(timeout=ELECTION_TIMEOUT + 2):
            self._configure_heartbeat()

    def _handle_server_shutdown(self, msg):
        from_ip = msg.get("from")
        self.logger.system(f"Server {from_ip} shutting down")
        if self.election.is_leader():
            self.server_members = [ip for ip in self.server_members if ip != from_ip]
            self.election.update_ring(self.server_members)
            self.heartbeat.update_monitored_nodes(
                [ip for ip in self.server_members if ip != self.my_ip])

    def _get_alive_members(self):
        alive = sorted(set(list(self.discovery.get_alive_servers().keys()) + [self.my_ip]))
        self.server_members = alive
        self._known_members = set(alive)
        return alive

    def _alive_peers(self):
        """Reachable server peers for anti-entropy (excludes self + blocked peers)."""
        return [ip for ip in self.server_members
                if ip != self.my_ip and not self.is_blocked(ip)]

    # --- Demo: simulated network partition ---

    def is_blocked(self, ip):
        return ip in self._blocked_peers

    def partition(self, ip):
        self._blocked_peers.add(ip)
        self.logger.fault(f"PARTITION: now dropping all traffic to/from {ip}")

    def heal(self):
        healed = sorted(self._blocked_peers)
        self._blocked_peers.clear()
        self.logger.system(f"HEAL: restored traffic to/from {healed}")

    # --- Network helpers ---

    def _udp_chat_send(self, target_ip, msg_dict):
        if self.is_blocked(target_ip):
            return  # simulated partition
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(json.dumps(msg_dict).encode(), (target_ip, CHAT_PORT))
            sock.close()
        except Exception as e:
            self.logger.fault(f"UDP send to {target_ip}:{CHAT_PORT} failed: {e}")

    def _broadcast_to_servers(self, msg_dict):
        for ip in self.server_members:
            if ip != self.my_ip:
                self._udp_chat_send(ip, msg_dict)

    @staticmethod
    def _tcp_send(sock, data):
        sock.sendall(struct.pack("!I", len(data)) + data)

    @staticmethod
    def _tcp_recv(sock):
        header = ChatServer._recv_exact(sock, 4)
        if header is None:
            return None
        return ChatServer._recv_exact(sock, struct.unpack("!I", header)[0])

    @staticmethod
    def _recv_exact(sock, n):
        data = b""
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    # --- Status ---

    def get_status(self):
        snap = self.delivery.snapshot()
        return {
            "ip": self.my_ip,
            "node_id": self.node_id,
            "leader": self.election.get_leader(),
            "is_leader": self.election.is_leader(),
            "ring": self.election.ring,
            "members": self.server_members,
            "view_id": self.group_view.get_view_id(),
            "blocked": sorted(self._blocked_peers),
            "clients": list(self.client_names.values()),
            "delivered": snap["delivered"],
            "holdback": snap["holdback"],
            "vc": snap["vc"],
        }

    def print_history(self):
        order = self.delivery.converged_order()
        print(f"\n\033[93m{'='*60}")
        print(f"  CONVERGED ORDER ({len(order)} messages) — identical on every replica")
        print(f"{'='*60}\033[0m")
        for i, e in enumerate(order, 1):
            print(f"  {i:3}. [LT{e['lamport_ts']:<3} {e['origin_id']}#{e['origin_counter']}] "
                  f"{e['username']}: {e['text']}")
        print(f"\033[93m{'='*60}\033[0m\n")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Distributed Chat Server (AP)")
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
    print(f"    status            - show server + causal-delivery state")
    print(f"    history           - print the converged total order")
    print(f"    partition <ip>    - (demo) drop all traffic to/from <ip>")
    print(f"    heal              - (demo) restore all blocked traffic")
    print(f"    quit              - shut down server")
    print(f"{'─'*60}\n")

    while True:
        try:
            parts = input().strip().split()
            if not parts:
                continue
            cmd = parts[0].lower()
            if cmd == "status":
                s = server.get_status()
                print(f"\n{'='*44}")
                print(f"  Server Status — {s['node_id']}")
                print(f"{'='*44}")
                print(f"  Leader:    {s['leader']} {'(ME)' if s['is_leader'] else ''}")
                print(f"  Ring:      {' -> '.join(s['ring'])}")
                print(f"  Members:   {s['members']}  (view #{s['view_id']})")
                if s['blocked']:
                    print(f"  BLOCKED:   {s['blocked']}  <-- simulated partition active")
                print(f"  Clients:   {s['clients']}")
                print(f"  Delivered: {s['delivered']} msgs   Held back: {s['holdback']}")
                print(f"  VectorClk: {s['vc']}")
                print(f"{'='*44}\n")
            elif cmd == "history":
                server.print_history()
            elif cmd == "partition" and len(parts) > 1:
                server.partition(parts[1])
            elif cmd == "heal":
                server.heal()
            elif cmd in ("quit", "exit", "q"):
                server.stop()
                break
        except (EOFError, KeyboardInterrupt):
            server.stop()
            break


if __name__ == "__main__":
    main()
