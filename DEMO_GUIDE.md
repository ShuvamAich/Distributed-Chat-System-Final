# ResiChat — Live Demo Guidebook

A step-by-step script for demonstrating all four required distributed-systems
properties to the professor. Follow it top to bottom; each section says **what to
do**, **what to watch**, and **what to say**.

The design you are demonstrating is **AP (availability-first)**: any server accepts
a message and spreads it peer-to-peer, the elected leader is only a coordinator
(liveness + membership) and is **off the message path**, ordering is **causal**
converging to an **eventual total order**, and the chat stays available through
crashes and network partitions. (Full rationale: `proposal.md`.)

---

## 0. The four properties and where each is shown

| # | Property | Demo section |
|---|----------|--------------|
| 1 | Dynamic discovery | §4 |
| 2 | Voting (leader election) | §5 |
| 3 | Fault tolerance (server crash, leader crash, partition + re-election) | §6 |
| 4 | Message ordering | §7 |

---

## 1. Setup (recommended: two machines on the same LAN)

Both machines must be on the **same IPv4 subnet** (e.g. both on the same Wi-Fi/
switch). Each machine runs a **server** and one or more **chat clients**.

**Prerequisites:** Python 3.7+ (standard library only — no installs). On Windows
first run `set PYTHONIOENCODING=utf-8` so the colored logs render.

**Firewall:** allow inbound **UDP 5972, 7001, 7002, 7003** and **TCP 7004** on both
machines. (On first run the OS may pop a "allow incoming connections" prompt —
accept it.)

### Launch order

```
# Machine A
python run_server.py                 # auto-detects its LAN IP
python run_client.py --username Alice

# Machine B
python run_server.py
python run_client.py --username Bob
```

Note each server's IP from its startup banner ("CHAT SERVER STARTING AT <ip>") —
call them **IP_A** and **IP_B**. You'll need them for the partition demo.

### Single-machine fallback (if you only have one computer)

A single server with multiple clients shows discovery, ordering, and client
reconnect, but **not** the multi-server behaviors (election among peers, partition):

```
python run_server.py --ip 127.0.0.1
python run_client.py --username Alice --server 127.0.0.1
python run_client.py --username Bob   --server 127.0.0.1
```

For a true multi-server cluster on one Mac, add loopback aliases first (one-time,
needs sudo), then give each server a distinct IP:
```
sudo ifconfig lo0 alias 127.0.0.2 up        # Linux: 127.0.0.2 already works
python run_server.py --ip 127.0.0.1
python run_server.py --ip 127.0.0.2
```

---

## 2. Reading the logs

Every server/client prints timestamped, color-coded events. Point the professor at
these during the demo:

| Color tag | Meaning |
|-----------|---------|
| `[DISCOVERY]` cyan | a peer was heard / broadcast sent |
| `[ELECTION]` magenta | LCR ring votes and leader announcements |
| `[VIEW]` blue | group-view (membership) change installed |
| `[HEARTBEAT]` gray | liveness pings |
| `[MESSAGE]` green | a chat message originated / gossiped |
| `[CAUSAL]` yellow | a message stamped with its vector clock |
| `[GOSSIP]` blue | anti-entropy gap-fill (catch-up) |
| `[FAULT]` red | a crash/partition detected, or a force-release |

## 3. Command reference

**Server console:** `status` · `history` · `partition <ip>` · `heal` · `quit`
**Client console:** `/status` · `/history` · `/quit`

- `status` (server) shows: leader, ring, members + view #, blocked peers, delivered
  count, hold-back count, and the vector clock.
- `history` (server) and `/history` (client) print the **converged total order** —
  the key artifact for the ordering demo.

---

## 4. Demo 1 — Dynamic Discovery

**Do:**
1. Start server A only. Run `status` → Members shows just `[IP_A]`.
2. Start server B. Within ~3–6 s run `status` on **both** → Members now shows
   `[IP_A, IP_B]` on each, with a bumped `view #`.
3. Start the clients (no server IP typed). Each prints `Found server: <ip>` and
   `Connected to chat!`.

**Watch:** `[DISCOVERY] Discovered new node` on each server; `[VIEW] View #n
installed` when membership grows.

**Say:** "Discovery is zero-configuration UDP broadcast — no addresses are typed.
Each server announces itself every 3 seconds; peers remember who they've heard from
and age out anyone who goes silent. A new server is absorbed with no restart, and —
because the coordinator is off the message path — adding a server never pauses the
live chat."

---

## 5. Demo 2 — Voting (LCR Leader Election)

**Do:**
1. With both servers up, run `status` on each. Both show the **same Leader** — the
   one with the **highest IP** — and the same Ring.
2. Scroll the server logs to the `ELECTION RESULT` banner.

**Watch:** `[ELECTION]` lines showing votes travelling the ring (`Received ring
message: mid=…`, `Forwarding to …`), ending in `*** LEADER ELECTED ***`.

**Say:** "Servers form a logical ring sorted by IP and run the LeLann-Chang-Roberts
algorithm: each forwards the higher candidate, and the highest IP completes the loop
and declares itself leader — everyone agrees with no referee. This ring exists
**only for voting**; chat messages never travel it. The leader is just a coordinator
for liveness and membership — it is deliberately kept off the message path, which is
why an election can never freeze the chat."

---

## 6. Demo 3 — Fault Tolerance

> Tip: before each kill, have Alice and Bob exchange a couple of messages so the
> room is "live." Timings below assume the defaults (heartbeat timeout ~6 s, a
> silent peer ages out of discovery in ~9 s).

### 6a. Kill a NON-leader server

**Do:**
1. Identify the non-leader server from `status`. Press **Ctrl+C** on it.
2. Immediately keep chatting from the client on the *surviving* server — messages
   flow with **no pause**.
3. After ~6 s, the leader logs `[FAULT] NODE FAILURE DETECTED` and installs a new
   view.
4. **Restart** that server (`python run_server.py` again). Watch `[GOSSIP]` lines —
   anti-entropy catches it up. Run `history` on it → it now matches the others.

**Say:** "A server crashing doesn't pause anyone else — every server is a full
replica and the leader isn't in the message path. When it comes back it's a fresh
incarnation and catches up automatically via anti-entropy gap-fill — no central
retransmission."

### 6b. Kill the LEADER

**Do:**
1. From `status`, find the leader. Press **Ctrl+C** on it.
2. From a client on **another** server, send messages during the gap — **the chat
   keeps flowing**. (Clients that were connected to the dead leader will auto-
   reconnect to another server within ~5 s and catch up — that reconnect is the only
   visible blip, and only for *those* clients.)
3. After ~6 s the followers log `[FAULT] LEADER FAILURE DETECTED` and start a new
   election; `status` on a survivor shows a **new leader**.

**Say:** "This is the headline AP property. The coordinator died, yet the
conversation never stopped — because message delivery is peer-to-peer and needs no
leader. A new coordinator is elected in the background; only control-plane tasks
waited, never the chat."

### 6c. Network Partition + independent re-election + heal

**Do:**
1. On **server A** console: `partition IP_B`. On **server B** console:
   `partition IP_A`. (Each now drops all traffic to/from the other — discovery,
   votes, heartbeats, chat, gossip.)
2. Run `status` on each: within ~10 s each shows **itself as leader**, Members =
   just itself, and `BLOCKED: [other-ip]`.
3. Keep chatting on **both** sides. Alice (on A) and Bob (on B) each see their own
   side's messages but **not** the other's — the sides are isolated.
4. **Heal:** type `heal` on **both** servers.
5. Within ~4–8 s watch `[GOSSIP]` reconcile. Then run `history` on **both** servers
   → the output is **identical**, including the messages sent during the split.

**Say:** "Each side kept serving the users it could reach and elected its own
coordinator. Because the coordinator is off the message path, two coordinators can't
corrupt anything — they just manage their own side. On heal, anti-entropy exchanges
what each side missed, the views merge, a single leader is re-elected, and both
replicas converge to the exact same conversation. Available during the split, fully
consistent again after it — the essence of AP."

---

## 7. Demo 4 — Message Ordering

### 7a. Causal order (a reply never precedes its cause)

**Do:** From Alice: `what time is the meeting?` Then from Bob: `3 PM`. On **every**
client and in every server's `history`, Alice's question always appears before
Bob's answer.

**Say:** "Each message carries a **vector clock** recording what its sender had
already seen. A server shows a message only once its causal dependencies are
delivered, so a reply is never shown before the message it answers — stronger than
FIFO."

### 7b. Eventual total order (all replicas converge identically)

**Do:**
1. Have Alice and Bob type several messages at roughly the same time (concurrent).
2. Run `/history` on **both** clients and `history` on **both** servers.
3. Show the four outputs are **identical**, line for line.

**Say:** "Concurrent messages have no natural order, so every replica applies the
same deterministic tiebreaker — `(Lamport timestamp, server-id)` — which respects
causality and converges all replicas to one identical total order. We never block
to agree on order before showing a message — that's what would cost availability —
so the order is reached a moment *after* delivery, not before."

> The `LT` number shown next to each message is its Lamport timestamp — the primary
> sort key. Point out that `history` is sorted by it.

### 7c. (Optional) Deterministic proof

If the professor wants the guarantees shown without relying on timing, run:
```
python test_local.py
```
It prints 18 passing checks: causal hold-back, force-release liveness, anti-entropy
gap-fill, concurrent-message tiebreaker determinism, and partition-merge
convergence.

---

## 8. Timing cheat-sheet

| Event | Expect within |
|-------|---------------|
| New server discovered | ~3–6 s |
| Server/leader crash detected | ~6 s |
| Silent peer aged out of discovery | ~9 s |
| Anti-entropy catch-up after rejoin/heal | ~4–8 s |
| Client reconnect after its server dies | ~5 s |

## 9. Troubleshooting

- **Servers don't find each other:** same subnet? Firewall allowing UDP
  5972/7001/7002/7003 and TCP 7004? Some corporate/guest Wi-Fi blocks broadcast —
  use a phone hotspot or a direct switch.
- **Client can't auto-discover:** connect explicitly with
  `python run_client.py --username X --server <server-ip>`.
- **One machine only:** use the §1 fallback; skip §6b/§6c (they need ≥2 servers), or
  use loopback aliases for a full local cluster.
- **Colors look like `[92m`:** run `set PYTHONIOENCODING=utf-8` (Windows) / use a
  normal ANSI terminal.

## 10. One-line talking points (per property)

- **Discovery:** "Zero-config UDP broadcast; servers absorb newcomers with no restart."
- **Voting:** "LCR ring election, highest IP wins; the ring is for voting only."
- **Fault tolerance:** "Crashes and partitions never pause the chat — only the
  coordinator's control-plane tasks wait; anti-entropy heals the gaps."
- **Ordering:** "Causal delivery via vector clocks, converging to one identical total
  order via a Lamport-timestamp tiebreaker — availability first, never blocking."
