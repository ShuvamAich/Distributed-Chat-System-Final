"""
Demo Script — launches servers and clients for demonstration.

For multi-machine demo: run servers on different machines on the same LAN.
For single-machine demo: use --ip 127.0.0.x with different loopback addresses.
"""

import subprocess
import sys
import os
import time
import platform

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def print_header():
    print("\033[96m" + "=" * 70)
    print("   DISTRIBUTED CHAT SYSTEM - DEMO  (AP: availability-first)")
    print("   Discovery: UDP Broadcast (broadcastsender/listener pattern)")
    print("   Election:  LCR Ring Algorithm (lcr-template.py) - CONTROL PLANE ONLY")
    print("   Messaging: peer-to-peer mesh, NO leader on the message path")
    print("   Ordering:  Causal (vector clocks) -> eventual total order")
    print("=" * 70 + "\033[0m\n")


def print_menu():
    print("\033[93mOptions:\033[0m")
    print("  1. Launch server (new terminal)")
    print("  2. Launch client (new terminal)")
    print("  3. Show architecture")
    print("  4. Show demo instructions")
    print("  5. Exit")
    print()


def show_architecture():
    print("""
\033[96m
  ┌─────────────┐    UDP Broadcast     ┌─────────────┐
  │  Client A   │◄── (Discovery) ──────►│  Client B   │
  └──────┬──────┘    255.255.255.255    └──────┬──────┘
         │ TCP                                  │ TCP
  ┌──────▼──────────────────────────────────────▼──────┐
  │              SERVER MESH (peer-to-peer)             │
  │                                                     │
  │  ┌──────────┐  UDP   ┌──────────┐  UDP   ┌──────────┐
  │  │ Server 1 │◄──────►│ Server 2 │◄──────►│ Server 3 │
  │  └──────────┘        └──────────┘        └──────────┘
  │   every server accepts + shows + gossips a message   │
  │   (the elected leader does liveness+membership ONLY, │
  │    and is deliberately OFF the message path)         │
  └──────────────────────────────────────────────────────┘

  Discovery:    UDP Broadcast (broadcastsender.py pattern)
  Ring (votes): Sorted by binary IP (ring.py pattern), LCR (lcr-template.py)
  Chat:         TCP client<->server, UDP server<->server mesh
  Ordering:     Vector clocks (causal) + Lamport tiebreaker (total order)
  Reliability:  Decentralized anti-entropy gap-fill (no central ACK/resend)
\033[0m""")


def show_instructions():
    print("""
\033[93mTwo-Machine Demo (recommended — both machines act as servers):\033[0m

  Machine 1: python run_server.py            (auto-detects its LAN IP)
  Machine 2: python run_server.py
  Machine 1: python run_client.py --username Alice
  Machine 2: python run_client.py --username Bob

  Both machines must be on the same LAN subnet. Servers discover each other
  via UDP broadcast; clients discover a server automatically. Scales to any
  number of servers/clients — nothing is hardcoded.

\033[93mSingle-Machine Smoke Test (one server + clients):\033[0m

  Terminal 1: python run_server.py --ip 127.0.0.1
  Terminal 2: python run_client.py --username Alice --server 127.0.0.1
  Terminal 3: python run_client.py --username Bob   --server 127.0.0.1
  (The full multi-server mesh is best shown across two real machines, because
   discovery and the mesh share fixed UDP ports.)

\033[93mDemo Scenarios (the four required properties):\033[0m

  1. DYNAMIC DISCOVERY — start the servers; 'status' on each shows both members.
     Start clients; they connect with no addresses typed.
  2. VOTING (LCR) — 'status' shows the same elected leader (highest IP) on both.
  3. FAULT TOLERANCE
     a) Kill a NON-leader server: chats keep flowing; restart it and watch
        anti-entropy ('GOSSIP' logs) catch it back up.
     b) Kill the LEADER: chat KEEPS FLOWING (leader is off the message path);
        a new leader is elected in the background.
     c) PARTITION: on each server type 'partition <other-ip>'. Both sides keep
        chatting and each elects its own leader. Then 'heal' on both — anti-
        entropy reconciles and 'history' converges identically on both.
  4. ORDERING — delay a reply's cause to see causal hold-back; send concurrent
     messages from both machines, then run 'history' on each server and confirm
     the converged order is identical.

\033[93mServer commands:\033[0m  status | history | partition <ip> | heal | quit
\033[93mClient commands:\033[0m  /status | /history | /quit
""")


def launch_server(ip=None):
    cmd = [sys.executable, os.path.join(SCRIPT_DIR, "run_server.py")]
    if ip:
        cmd += ["--ip", ip]

    if platform.system() == "Windows":
        subprocess.Popen(["start", "cmd", "/k"] + cmd, shell=True)
    else:
        subprocess.Popen(["gnome-terminal", "--", "bash", "-c",
                          f"cd {SCRIPT_DIR} && {' '.join(cmd)}; read -p 'Press Enter...'"])


def launch_client(username=None, server=None):
    cmd = [sys.executable, os.path.join(SCRIPT_DIR, "run_client.py")]
    if username:
        cmd += ["--username", username]
    if server:
        cmd += ["--server", server]

    if platform.system() == "Windows":
        subprocess.Popen(["start", "cmd", "/k"] + cmd, shell=True)
    else:
        subprocess.Popen(["gnome-terminal", "--", "bash", "-c",
                          f"cd {SCRIPT_DIR} && {' '.join(cmd)}; read -p 'Press Enter...'"])


def main():
    print_header()

    while True:
        print_menu()
        try:
            choice = input("\033[96mSelect: \033[0m").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if choice == "1":
            ip = input("  Server IP (Enter for auto-detect): ").strip() or None
            launch_server(ip)
            print(f"  Server launched.\n")
        elif choice == "2":
            name = input("  Username: ").strip() or "User1"
            server = input("  Server IP (Enter for auto-discover): ").strip() or None
            launch_client(name, server)
            print(f"  Client launched.\n")
        elif choice == "3":
            show_architecture()
        elif choice == "4":
            show_instructions()
        elif choice == "5":
            break
        else:
            print("  Invalid option.\n")

    print("Goodbye!")


if __name__ == "__main__":
    main()
