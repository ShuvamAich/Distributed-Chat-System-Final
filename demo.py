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
    print("   DISTRIBUTED CHAT SYSTEM - DEMO")
    print("   Discovery: UDP Broadcast (broadcastsender/listener pattern)")
    print("   Election: LCR Ring Algorithm (lcr-template.py pattern)")
    print("   Communication: UDP (servers) + TCP (clients)")
    print("   Ordering: Causal (Vector Clocks) + Total (Leader Sequencer)")
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
         │                                      │
  ┌──────▼──────────────────────────────────────▼──────┐
  │                 SERVER RING (LCR)                    │
  │                                                     │
  │  ┌──────────┐  UDP   ┌──────────┐  UDP   ┌──────────┐
  │  │Server .1 │◄──────►│Server .5 │◄──────►│Server .10│
  │  │(Follower)│        │(Follower)│        │ (LEADER) │
  │  └──────────┘        └──────────┘        └──────────┘
  │       ▲                    ▲                    ▲     │
  │       └──── Heartbeats ────┴──── Ring Votes ────┘     │
  └───────────────────────────────────────────────────────┘

  Discovery: UDP Broadcast (broadcastsender.py pattern)
  Ring:      Sorted by binary IP (ring.py pattern)
  Election:  LCR via UDP ring (lcr-template.py pattern)
  Chat:      TCP client->server, UDP server->server
  Ordering:  Vector clocks (lamport_clock.py extended)
\033[0m""")


def show_instructions():
    print("""
\033[93mMulti-Machine Demo (recommended):\033[0m

  Machine 1: python run_server.py
  Machine 2: python run_server.py
  Machine 3: python run_server.py

  Any machine: python run_client.py --username Alice
  Any machine: python run_client.py --username Bob

  All machines must be on the same LAN subnet.
  Servers discover each other automatically via UDP broadcast.
  Clients discover servers automatically.

\033[93mSingle-Machine Demo:\033[0m

  Terminal 1: python run_server.py --ip 127.0.0.1
  Terminal 2: python run_server.py --ip 127.0.0.2
  Terminal 3: python run_server.py --ip 127.0.0.3
  Terminal 4: python run_client.py --username Alice --server 127.0.0.1
  Terminal 5: python run_client.py --username Bob --server 127.0.0.2

\033[93mDemo Scenarios:\033[0m

  1. Watch server terminals for leader election (LCR ring votes)
  2. Type messages in client terminals — observe sequence numbers
  3. Kill the LEADER terminal — watch re-election happen
  4. Messages continue flowing after new leader elected
  5. Type 'status' in any server to see cluster state
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
