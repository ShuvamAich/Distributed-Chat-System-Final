"""
Chat Client — connects to the distributed chat server cluster.
Based on simpleclient.py example pattern.

Uses:
- UDP broadcast listener to discover servers.
- TCP connection to send/receive chat messages (like simpleclient.py).

Client workflow:
1. Listens for server broadcast announcements.
2. Connects to a discovered server via TCP.
3. Sends JOIN request.
4. Receives approval + chat history.
5. Sends/receives messages interactively.
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
from common.logger import SystemLogger
from common.network import get_local_ip
from common.lamport_clock import LamportClock


class ChatClient:
    """Interactive chat client with automatic server discovery."""

    def __init__(self, username=None, server_ip=None):
        self.username = username or f"User-{os.getpid() % 1000}"
        self.my_ip = get_local_ip()
        self.logger = SystemLogger(self.my_ip, "CLIENT")
        self.server_ip = server_ip  # can be specified manually
        self.server_socket = None
        self.joined = False
        self.leader_ip = None
        self.lamport_clock = LamportClock()
        self.message_history = []
        self.last_received_seq = 0  # track last seq for gap detection on reconnect
        self._send_buffer = []  # messages queued while disconnected
        self._lock = threading.Lock()
        self._running = False

    def start(self):
        """Start the client — discover and connect."""
        self._running = True
        self.logger.banner(f"CHAT CLIENT: {self.username}")
        self.logger.system(f"My IP: {self.my_ip}")
        self.logger.separator()

        if self.server_ip:
            # Connect directly to specified server
            self._connect_to_server(self.server_ip)
        else:
            # Discover server via broadcast
            self.logger.discovery("Listening for server broadcasts...")
            threading.Thread(target=self._discover_and_connect, daemon=True).start()

    def stop(self):
        """Gracefully disconnect."""
        self._running = False
        if self.server_socket and self.joined:
            try:
                disconnect = json.dumps({
                    "type": MSG_CLIENT_DISCONNECT,
                    "username": self.username
                }).encode()
                self._tcp_send(self.server_socket, disconnect)
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
        """Send a chat message to the server. Buffers if disconnected."""
        # Increment Lamport clock before sending
        ts = self.lamport_clock.increment()
        self.logger.order(f"Send Lamport TS: {ts}")

        msg_data = {
            "type": MSG_CHAT_MESSAGE,
            "text": text,
            "username": self.username,
            "ts": ts,
        }

        if not self.joined or not self.server_socket:
            # Buffer for retry when reconnected
            with self._lock:
                self._send_buffer.append(msg_data)
            print("  [Buffered — will send when reconnected]")
            return

        try:
            self._tcp_send(self.server_socket, json.dumps(msg_data).encode())
        except Exception:
            # Buffer the failed message for retry
            with self._lock:
                self._send_buffer.append(msg_data)
            print("  [Send failed — buffered. Reconnecting...]")
            self.joined = False
            threading.Thread(target=self._reconnect, daemon=True).start()

    def _flush_send_buffer(self):
        """Send all buffered messages after reconnection."""
        with self._lock:
            buffer = list(self._send_buffer)
            self._send_buffer.clear()

        if buffer:
            self.logger.reliability(f"Flushing {len(buffer)} buffered messages")
            print(f"  [Sending {len(buffer)} buffered message(s)...]")

        for msg_data in buffer:
            try:
                self._tcp_send(self.server_socket, json.dumps(msg_data).encode())
                self.logger.reliability(f"Buffered message sent: \"{msg_data.get('text', '')}\"")
            except Exception:
                # Re-buffer if still failing
                with self._lock:
                    self._send_buffer.append(msg_data)
                break

    def _discover_and_connect(self):
        """Listen for broadcast announcements and connect to first server found."""
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        listen_socket.bind(("0.0.0.0", BROADCAST_PORT))
        listen_socket.settimeout(2.0)

        while self._running and not self.joined:
            try:
                data, addr = listen_socket.recvfrom(BUFFER_SIZE)
                if data:
                    msg = json.loads(data.decode())
                    if msg.get("type") == MSG_DISCOVERY_ANNOUNCE:
                        server_ip = msg.get("ip", addr[0])
                        if server_ip != self.my_ip:
                            self.logger.discovery(
                                f"Found server: {server_ip}")
                            listen_socket.close()
                            self._connect_to_server(server_ip)
                            return
            except socket.timeout:
                self.logger.discovery("No server found yet, listening...")
                continue
            except Exception as e:
                if self._running:
                    self.logger.fault(f"Discovery error: {e}")
                break

        listen_socket.close()

    def _connect_to_server(self, server_ip):
        """Establish TCP connection to a server (like simpleclient.py)."""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.settimeout(5.0)
            self.server_socket.connect((server_ip, TCP_PORT))
            self.server_socket.settimeout(None)
            self.server_ip = server_ip

            self.logger.system(f"Connected to server {server_ip}:{TCP_PORT}")

            # Send join request (include last_seq for gap recovery on reconnect)
            join_msg = json.dumps({
                "type": MSG_JOIN_REQUEST,
                "username": self.username,
                "ip": self.my_ip,
                "last_seq": self.last_received_seq,
            }).encode()
            self._tcp_send(self.server_socket, join_msg)

            # Start receive thread
            t = threading.Thread(target=self._receive_loop, daemon=True)
            t.start()

        except Exception as e:
            self.logger.fault(f"Connection to {server_ip} failed: {e}")
            if not self.server_ip:
                threading.Thread(target=self._discover_and_connect, daemon=True).start()

    def _receive_loop(self):
        """Receive messages from server."""
        while self._running:
            try:
                data = self._tcp_recv(self.server_socket)
                if data is None:
                    break
                msg = json.loads(data.decode())
                self._handle_server_message(msg)
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
        """Handle messages from server."""
        msg_type = msg.get("type")

        if msg_type == MSG_JOIN_APPROVED:
            self.joined = True
            self.leader_ip = msg.get("leader")
            self.logger.system(
                f"Joined successfully! Leader: {self.leader_ip}")
            print(f"\n{'='*50}")
            print(f"  Connected to chat! (Server: {self.server_ip})")
            print(f"  Leader: {self.leader_ip}")
            print(f"  Type messages and press Enter to send.")
            print(f"  Commands: /status, /history, /quit")
            print(f"{'='*50}\n")

            # Flush any buffered messages from before disconnect
            threading.Thread(target=self._flush_send_buffer, daemon=True).start()

        elif msg_type == MSG_SYNC_RESPONSE:
            messages = msg.get("messages", [])
            if messages:
                print(f"\n  --- Chat History ({len(messages)} messages) ---")
                for m in messages:
                    self._display_message(m, is_history=True)
                print(f"  --- End History ---\n")

        elif msg_type == MSG_CHAT_ORDERED:
            # Update Lamport clock on receive: max(local, received) + 1
            received_ts = msg.get("ts", 0)
            if received_ts:
                self.lamport_clock.update(received_ts)
                self.logger.order(f"Recv Lamport TS updated: {self.lamport_clock.get()}")
            # Track sequence for gap recovery on reconnect
            seq = msg.get("seq")
            if seq and isinstance(seq, int):
                self.last_received_seq = max(self.last_received_seq, seq)
            self._display_message(msg)
            with self._lock:
                self.message_history.append(msg)

    def _display_message(self, msg, is_history=False):
        """Display a chat message in the terminal."""
        text = msg.get("text", "")
        username = msg.get("username", "?")
        seq = msg.get("seq", "?")
        lamport_ts = msg.get("ts", "?")
        wall_ts = msg.get("timestamp")
        is_system = msg.get("system", False)

        time_str = datetime.fromtimestamp(wall_ts).strftime("%H:%M:%S") if wall_ts else "??:??:??"

        RESET = "\033[0m"
        GREEN = "\033[92m"
        CYAN = "\033[96m"
        GRAY = "\033[90m"

        if is_system:
            print(f"  {GRAY}[{time_str}] [Seq#{seq}|LT:{lamport_ts}] {text}{RESET}")
        elif username == self.username:
            print(f"  {GREEN}[{time_str}] [Seq#{seq}|LT:{lamport_ts}] {username} (you): {text}{RESET}")
        else:
            print(f"  {CYAN}[{time_str}] [Seq#{seq}|LT:{lamport_ts}] {username}: {text}{RESET}")

    def _reconnect(self):
        """Try to reconnect to any available server."""
        time.sleep(RECONNECT_INTERVAL)
        if self._running and not self.joined:
            self._discover_and_connect()

    # --- TCP Helpers (length-prefixed framing) ---

    @staticmethod
    def _tcp_send(sock, data):
        length = struct.pack("!I", len(data))
        sock.sendall(length + data)

    @staticmethod
    def _tcp_recv(sock):
        header = ChatClient._recv_exact(sock, 4)
        if header is None:
            return None
        length = struct.unpack("!I", header)[0]
        return ChatClient._recv_exact(sock, length)

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
    parser = argparse.ArgumentParser(description="Distributed Chat Client")
    parser.add_argument("--username", "-u", type=str, default=None,
                        help="Chat username")
    parser.add_argument("--server", "-s", type=str, default=None,
                        help="Server IP (skip discovery)")
    args = parser.parse_args()

    client = ChatClient(username=args.username, server_ip=args.server)

    def shutdown(sig, frame):
        print("\n")
        client.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    client.start()

    # Wait for connection
    while not client.joined and client._running:
        time.sleep(0.5)

    if not client._running:
        return

    # Interactive loop
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
                    print(f"\n  Username: {client.username}")
                    print(f"  Server: {client.server_ip}")
                    print(f"  Leader: {client.leader_ip}")
                    print(f"  Messages: {len(client.message_history)}")
                    print(f"  Lamport Clock: {client.lamport_clock.get()}\n")
                elif cmd == "/history":
                    with client._lock:
                        for m in client.message_history[-20:]:
                            client._display_message(m, is_history=True)
                else:
                    print(f"  Commands: /status, /history, /quit")
            else:
                client.send_message(text)

        except (EOFError, KeyboardInterrupt):
            client.stop()
            break


if __name__ == "__main__":
    main()
