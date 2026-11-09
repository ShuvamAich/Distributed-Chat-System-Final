"""
Chat Client — connects to the distributed chat server cluster (AP design).
Based on the simpleclient.py example pattern.

The client is deliberately thin: it sends raw text and renders whatever its
server causally delivers. It does NOT assign clocks or sequence numbers — the
servers own ordering. It keeps a per-origin "last-seen marker" (a version
vector) so that on reconnect — even to a DIFFERENT server — it is caught up on
exactly what it missed, preserving causal context.

A short reorder window buffers freshly-delivered messages so that concurrent
messages from different machines settle into the converged order before they are
printed; `/history` prints the same deterministic order.
"""

import sys
import os
import socket
import json
import struct
import threading
import time
import signal
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.constants import *
from common.network import get_broadcast_addresses, get_local_ip
from common.logger import SystemLogger


def _sort_key(m):
    return (m.get("lamport_ts", 0), m.get("origin_id", ""), m.get("origin_counter", 0))


class ChatClient:
    """Interactive chat client with automatic server discovery."""

    def __init__(self, username=None, server_ip=None, server_port=None):
        self.username = username or f"User-{os.getpid() % 1000}"
        self.my_ip = get_local_ip()
        self.logger = SystemLogger(self.my_ip, "CLIENT")
        self.server_ip = server_ip
        self._server_port = server_port or TCP_PORT
        self.server_socket = None
        self.joined = False
        self.leader_ip = None

        self.marker = {}            # origin_id -> highest counter seen (last-seen marker)
        self._seen_ids = set()      # dedup across reconnects
        self.message_history = []   # all displayed messages (for /history)
        self._reorder = []          # [(arrival_time, msg)] awaiting the reorder window
        self._send_buffer = []      # messages typed while disconnected
        self._lock = threading.Lock()
        self._running = False

    def start(self):
        self._running = True
        self.logger.banner(f"CHAT CLIENT: {self.username}")
        self.logger.system(f"My IP: {self.my_ip}")
        self.logger.separator()
        threading.Thread(target=self._reorder_loop, daemon=True).start()

        if self.server_ip:
            self._connect_to_server(self.server_ip, self._server_port)
        else:
            self.logger.discovery("Listening for server broadcasts...")
            threading.Thread(target=self._discover_and_connect, daemon=True).start()

    def stop(self):
        self._running = False
        if self.server_socket and self.joined:
            try:
                self._tcp_send(self.server_socket, json.dumps({
                    "type": MSG_CLIENT_DISCONNECT, "username": self.username}).encode())
            except Exception:
                pass
            time.sleep(0.3)
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
        self.logger.system("Client stopped")

    def send_message(self, text):
        """Send a chat message. The server does all stamping/ordering."""
        msg_data = {"type": MSG_CHAT_MESSAGE, "text": text, "username": self.username}
        if not self.joined or not self.server_socket:
            with self._lock:
                self._send_buffer.append(msg_data)
            print("  [Buffered — will send when reconnected]")
            return
        try:
            self._tcp_send(self.server_socket, json.dumps(msg_data).encode())
        except Exception:
            with self._lock:
                self._send_buffer.append(msg_data)
            print("  [Send failed — buffered. Reconnecting...]")
            self.joined = False
            threading.Thread(target=self._reconnect, daemon=True).start()

    def _flush_send_buffer(self):
        with self._lock:
            buffer = list(self._send_buffer)
            self._send_buffer.clear()
        if buffer:
            print(f"  [Sending {len(buffer)} buffered message(s)...]")
        for msg_data in buffer:
            try:
                self._tcp_send(self.server_socket, json.dumps(msg_data).encode())
            except Exception:
                with self._lock:
                    self._send_buffer.append(msg_data)
                break

    def _discover_and_connect(self):
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        listen_socket.bind(("0.0.0.0", BROADCAST_PORT))
        listen_socket.settimeout(2.0)

        def send_probe():
            probe = json.dumps({"type": MSG_DISCOVERY_REQUEST,
                                "ip": self.my_ip, "role": ROLE_CLIENT}).encode()
            for target in get_broadcast_addresses(self.my_ip):
                try:
                    listen_socket.sendto(probe, (target, BROADCAST_PORT))
                except OSError as exc:
                    self.logger.fault(f"Discovery probe send failed: {exc}")

        send_probe()
        while self._running and not self.joined:
            try:
                data, addr = listen_socket.recvfrom(BUFFER_SIZE)
                if data:
                    msg = json.loads(data.decode())
                    if msg.get("type") in (MSG_DISCOVERY_ANNOUNCE, MSG_DISCOVERY_RESPONSE):
                        server_ip = msg.get("ip", addr[0])
                        server_port = msg.get("tcp_port") or TCP_PORT
                        self.logger.discovery(f"Found server: {server_ip}:{server_port}")
                        listen_socket.close()
                        self._connect_to_server(server_ip, server_port)
                        return
            except socket.timeout:
                send_probe()
                continue
            except Exception as e:
                if self._running:
                    self.logger.fault(f"Discovery error: {e}")
                break
        listen_socket.close()

    def _connect_to_server(self, server_ip, port=None):
        port = port or TCP_PORT
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.settimeout(5.0)
            self.server_socket.connect((server_ip, port))
            self.server_socket.settimeout(None)
            self.server_ip = server_ip
            self._server_port = port
            self.logger.system(f"Connected to server {server_ip}:{port}")

            # Join request carries the last-seen marker for cross-server catch-up.
            with self._lock:
                marker = dict(self.marker)
            self._tcp_send(self.server_socket, json.dumps({
                "type": MSG_JOIN_REQUEST,
                "username": self.username,
                "ip": self.my_ip,
                "marker": marker,
            }).encode())

            threading.Thread(target=self._receive_loop, daemon=True).start()
        except Exception as e:
            self.logger.fault(f"Connection to {server_ip} failed: {e}")
            try:
                self.server_socket.close()
            except Exception:
                pass
            self.server_socket = None
            if not self.server_ip:
                threading.Thread(target=self._discover_and_connect, daemon=True).start()

    def _receive_loop(self):
        while self._running:
            try:
                data = self._tcp_recv(self.server_socket)
                if data is None:
                    break
                self._handle_server_message(json.loads(data.decode()))
            except (ConnectionResetError, BrokenPipeError, OSError):
                break
            except Exception as e:
                if self._running:
                    self.logger.fault(f"Receive error: {e}")
                break
        if self._running:
            self.joined = False
            print("\n  [Connection lost. Reconnecting...]")
            threading.Thread(target=self._reconnect, daemon=True).start()

    def _handle_server_message(self, msg):
        msg_type = msg.get("type")

        if msg_type == MSG_JOIN_APPROVED:
            self.joined = True
            self.leader_ip = msg.get("leader")
            self.logger.system(f"Joined! Leader: {self.leader_ip}")
            print(f"\n{'='*50}")
            print(f"  Connected to chat! (Server: {self.server_ip})")
            print(f"  Leader: {self.leader_ip}  [AP — chat works even with no leader]")
            print(f"  Type messages and press Enter. Commands: /status /history /quit")
            print(f"{'='*50}\n")
            threading.Thread(target=self._flush_send_buffer, daemon=True).start()

        elif msg_type == MSG_SYNC_RESPONSE:
            messages = msg.get("messages", [])
            fresh = [m for m in messages if self._note(m)]
            if fresh:
                print(f"\n  --- Catch-up ({len(fresh)} messages) ---")
                for m in sorted(fresh, key=_sort_key):
                    self._print_message(m)
                print(f"  --- End catch-up ---\n")

        elif msg_type == MSG_CHAT_DELIVER:
            if self._note(msg):
                with self._lock:
                    self._reorder.append((time.time(), msg))

    def _note(self, msg):
        """Record a message in marker/history; return False if it's a duplicate."""
        mid = f"{msg.get('origin_id')}:{msg.get('origin_counter')}"
        with self._lock:
            if mid in self._seen_ids:
                return False
            self._seen_ids.add(mid)
            origin, ctr = msg.get("origin_id"), msg.get("origin_counter", 0)
            if ctr > self.marker.get(origin, 0):
                self.marker[origin] = ctr
            self.message_history.append(msg)
        return True

    def _reorder_loop(self):
        """Flush buffered messages older than the reorder window, in converged order."""
        while self._running:
            time.sleep(0.2)
            now = time.time()
            with self._lock:
                due = [(t, m) for (t, m) in self._reorder if now - t >= REORDER_WINDOW]
                for item in due:
                    self._reorder.remove(item)
            for _, m in sorted((d for d in due), key=lambda tm: _sort_key(tm[1])):
                self._print_message(m)

    def _print_message(self, msg):
        text = msg.get("text", "")
        username = msg.get("username", "?")
        lt = msg.get("lamport_ts", "?")
        is_system = msg.get("system", False)
        tag = f"[LT{lt}]"
        RESET, GREEN, CYAN, GRAY = "\033[0m", "\033[92m", "\033[96m", "\033[90m"
        if is_system:
            print(f"  {GRAY}{tag} {text}{RESET}")
        elif username == self.username:
            print(f"  {GREEN}{tag} {username} (you): {text}{RESET}")
        else:
            print(f"  {CYAN}{tag} {username}: {text}{RESET}")

    def _reconnect(self):
        time.sleep(RECONNECT_INTERVAL)
        if self._running and not self.joined:
            if self.server_ip:
                self._connect_to_server(self.server_ip, self._server_port)
            else:
                self._discover_and_connect()

    # --- TCP helpers (length-prefixed framing) ---

    @staticmethod
    def _tcp_send(sock, data):
        sock.sendall(struct.pack("!I", len(data)) + data)

    @staticmethod
    def _tcp_recv(sock):
        header = ChatClient._recv_exact(sock, 4)
        if header is None:
            return None
        return ChatClient._recv_exact(sock, struct.unpack("!I", header)[0])

    @staticmethod
    def _recv_exact(sock, n):
        data = b""
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Distributed Chat Client (AP)")
    parser.add_argument("--username", "-u", type=str, default=None, help="Chat username")
    parser.add_argument("--server", "-s", type=str, default=None,
                        help="Server IP (skip discovery)")
    parser.add_argument("--port", "-p", type=int, default=None,
                        help=f"Server TCP port (default: {TCP_PORT})")
    args = parser.parse_args()

    client = ChatClient(username=args.username, server_ip=args.server,
                        server_port=args.port)

    def shutdown(sig, frame):
        print("\n")
        client.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    client.start()
    while not client.joined and client._running:
        time.sleep(0.5)
    if not client._running:
        return

    while client._running:
        try:
            text = input()
            if not text:
                continue
            if text.startswith("/"):
                cmd = text.lower().strip()
                if cmd == "/quit":
                    client.stop()
                    break
                elif cmd == "/status":
                    with client._lock:
                        print(f"\n  Username: {client.username}")
                        print(f"  Server:   {client.server_ip}   Leader: {client.leader_ip}")
                        print(f"  Messages: {len(client.message_history)}")
                        print(f"  Marker:   {client.marker}\n")
                elif cmd == "/history":
                    with client._lock:
                        hist = sorted(client.message_history, key=_sort_key)
                    print(f"\n  --- Converged order ({len(hist)} messages) ---")
                    for m in hist:
                        client._print_message(m)
                    print(f"  --- End ---\n")
                else:
                    print(f"  Commands: /status, /history, /quit")
            else:
                client.send_message(text)
        except (EOFError, KeyboardInterrupt):
            client.stop()
            break


if __name__ == "__main__":
    main()
