Project Proposal — ResiChat
1. Project Description
ResiChat is a distributed group chat that runs across several computers on one local network. Its goal is the classic chat problem — messages must reach everyone, in a sensible order — while the system stays available to its users even when a server crashes or the network briefly splits. We build everything in plain Python (no Kafka, no ZooKeeper), so every mechanism is ours and visible. We demonstrate it with 2–3 servers and a few users on a LAN, which is enough to show every property.

Where we stand on CAP. We prioritise Availability and Partition-tolerance (AP). We deliberately do not provide linearizability — the strongest consistency model, where every operation appears to take effect instantly in one real-time order — because CAP shows it is incompatible with staying available during a partition (a linearizable chat would have to pause). Instead we provide causal consistency with eventual convergence: among the strongest guarantees compatible with always being available, and more than enough for a chat.

Honest scope. "State of the art" here means the distributed-systems design choices modern chats make — availability-first, causal ordering, eventual consistency, partition tolerance — not their product features (encryption, multi-datacenter, push, offline sync), which are out of scope. And "available" means precisely: you can always chat with the people your side can reach — not that every user everywhere stays connected during a split.

2. Architecture Model
Shape: a hybrid of client–server and peer-to-peer. Users (clients) connect to a server over TCP, a reliable, in-order link. The servers are equal peers — no permanent master — and talk to each other over UDP. Every server holds a full copy of the chat, so there is no single point whose loss takes the conversation down.

Server topology — two shapes for two jobs:

For leader election, the servers form a logical ring (sorted by address), around which the vote travels (§4). This ring exists only for voting.
For messaging and replication, the servers form a mesh — each can exchange messages with the others directly, peer-to-peer. Messages do not travel around the ring.
(At the edge, each server is a small hub for its own connected clients — those clients talk only to their server.)

Messaging flow. A user's message goes over TCP to whatever server it's connected to. That server accepts it immediately, shows it to its own users, and spreads it across the mesh to the other servers, which deliver it to their users. There is no central node every message must pass through — this is what keeps the chat available even when a server, or the coordinator, is briefly down.

Concurrency inside each server: multi-threading. The work is almost entirely waiting on the network (I/O-bound), so lightweight threads that share state are the right fit, and Python's GIL releases during socket I/O. Each server runs separate threads for accepting clients, reading from each client, discovery, election, liveness checks, and server-to-server messaging. Shared data (the message list, client connections, membership) is guarded by locks so the threads don't corrupt one another.

Replication. Each server is a full replica, storing the whole chat as an append-only list of messages (nothing is edited or deleted). Replicas reconcile by combining their lists; how they converge on the same order is covered in §7.

3. Dynamic Discovery
Automatic, zero-configuration, via UDP broadcast. Each server periodically announces "I'm here"; everyone listens and remembers who they've heard from and when. A new user finds a server and connects; a new server is heard and absorbed. No restart, no addresses typed in.

Scaling out stays available. Because the coordinator is off the message path, adding a server never pauses the live chat — existing users keep talking while the newcomer catches up in the background (§7). If the newcomer becomes the new coordinator, only control-plane tasks (admission, membership) pause briefly — never the conversation.

Why broadcast (and not other methods). Discovery means finding nodes you don't know yet, so one-to-one messaging is impossible — you'd need their address already. Multicast would work but needs group setup and router support that's unreliable on everyday LANs. Broadcast reaches everyone in one packet with zero config. Its one limit — staying on a single subnet — matches our LAN scope.

Honest limit: broadcast alone doesn't reliably notice a server that crashes silently; we also age out servers we haven't heard from within a timeout, and the coordinator's liveness checks (§5) handle the rest.

4. Voting (Leader Election)
LCR ring election. The servers form a logical ring sorted by address and pass a vote around; the highest address wins, and because everyone follows the same rule, they all agree with no referee. A fresh vote runs whenever the group changes or the coordinator stops responding.

Why LCR. Our servers already form a ring, so LCR reuses that structure with tiny logic (forward the higher, drop the lower, declare yourself if your own vote returns) that's easy to build and show step-by-step. Honest comparison: the Bully algorithm tolerates a crash during the election slightly better, but needs every server to message every higher server and doesn't use our ring. Neither is safe alone — both just pick the highest address — so we pair LCR with a timeout that rebuilds the ring if a vote is lost (LCR's main weakness).

Why the election can't freeze the chat. The election is a control-plane activity. If it stalls and the timeout rebuilds the ring, the live chat is unaffected, because message delivery is peer-to-peer and needs no current leader. Only the coordinator's tasks (§5) wait for the election to finish — a few seconds. This is exactly why we keep the leader off the message path.

5. Coordination
Some tasks need a single decision-maker. We handle these with an elected coordinator (the leader from §4), which performs two control-plane jobs:

Checking servers are alive — it sends periodic heartbeats and tracks who responds. (Answers "who checks whether servers are up?")
Managing membership — it keeps the authoritative list of which servers are in the cluster.
Why an elected coordinator, not a fixed one. A permanent coordinator would be a single point of failure. Any server can be elected (§4), and if it fails the others elect a new one — so coordination always has an owner without any server being irreplaceable.

Why this doesn't hurt availability. The coordinator is deliberately kept off the message path — it does not route, order, or relay chat messages. So if it's briefly down or being re-elected, the live chat keeps running; only the two coordination tasks pause.

(Alternative considered.) These tasks could be done without a leader — servers could track liveness peer-to-peer, membership could be eventually-consistent. We chose an elected coordinator for a single clear authority on membership, and because it lets us demonstrate leader election. A leaderless variant would be slightly more available but drops the election mechanism.

6. Fault Tolerance
Our failure handling keeps the conversation available in every case; only control-plane tasks ever wait.

A user drops → on reconnect it reports its last-seen marker, which both fetches the messages it missed and preserves its causal context, so causality survives even if it reconnects to a different server.
A server crashes → the other servers keep serving their users instantly, with no pause; when the crashed server restarts, it catches up on what it missed.
The coordinator crashes → the chat keeps flowing; a new coordinator is elected in the background, after which admission and membership resume.
The network splits → each side keeps serving the users it can reach — sending and reading within its side. Each isolated side, having lost contact with the coordinator, elects its own; because the coordinator is off the message path, two coordinators cannot corrupt the chat — they simply manage their own side's membership. The sides can't exchange messages while split, so users on opposite sides temporarily don't see each other. When the network heals, the sides exchange what they missed, their membership views merge, they re-elect to a single coordinator, and everyone converges to the full conversation. We stay available, but not consistent across the split until it heals — the essence of AP.
Crash detection: heartbeats. The coordinator pings servers to track who's alive, and any message from a peer also counts as a liveness sign. Honest note: our timeout is conservative to avoid false alarms, so a crash takes a few seconds to notice — the chat keeps running throughout.

No message loss — scoped honestly. A message is safe once it has reached at least one other server; from there the gap check (§7) spreads it everywhere. Honest limit: a message held only on a single server that crashes before it spreads can still be lost, because we keep data in memory. Full-cluster power loss is out of scope.

7. Reliable, Causally-Ordered Multicast
This section covers reliable delivery, ordering (including replication and clocks).

Reliable delivery. When a server receives a message, it shows it to its own users immediately and forwards it to the other servers over UDP (best-effort). Since UDP can drop packets, servers also periodically check each other for gaps: two servers compare the message IDs they hold and send each other anything missing. Every message has a unique ID, so a duplicate is simply ignored. This fills losses without a central confirm-and-resend authority, and a server that was down or cut off automatically catches up on the next check.

Ordering — causal delivery, converging to an eventual total order. The standard guarantees form a hierarchy, FIFO ⊂ causal ⊂ total. Our delivery guarantee is causal ordering — a message is shown only after the messages its sender had already seen, so a reply is never shown before the message it answers, and a sender's own messages stay in send-order (FIFO included). On top of that, for display consistency in a shared room, all servers apply the same deterministic (timestamp, server-id) tiebreaker to concurrent messages, so replicas converge to one identical order — an eventual total order. What we deliberately avoid is immediate (blocking) total order: we never wait for global agreement before showing a message, which is what would cost availability.

Important: the order respects causality. The final order is a linear extension of the causal order — causally-related messages always keep their causal order, and the tiebreaker only decides between concurrent (causally-unrelated) messages. We never sort purely by timestamp, which under clock skew could otherwise place a reply before its cause.

How we enforce it — vector clocks. Each message carries a vector clock: counters keyed by stable server IDs, recording what its sender had delivered. On arrival, a server shows a message only once it has delivered everything that message depends on; otherwise it holds the message briefly and releases it when the missing one arrives. A slot for a server not yet seen is treated as 0, so a new server joining (or a partition merging) simply adds entries — a case our demos exercise directly.

If a dependency is never received. Because a message can be lost (a single node crashing before it spreads), a held message's cause might never arrive. To avoid holding it forever, a held message is force-released after a timeout — accepting a one-time ordering exception rather than blocking a message permanently. This keeps the system live.

Replication and consistency. Every server keeps a full copy as a growing list of messages — nothing edited or deleted — so two servers reconcile just by combining their lists. There are no clashes, only gaps to close. Because all servers apply the same causal rules and the same tiebreaker, they converge to the same messages in the same order — eventual consistency. Limits: convergence assumes servers eventually reconnect; history is kept in memory and isn't trimmed; and editing/deleting messages is out of scope, since it would reintroduce clashes this approach avoids.

Honest cost. Holding a message until its cause arrives can delay showing it — a small latency, not a loss or a refusal, since the system always accepts messages immediately. And the eventual total order is reached a moment after a message arrives, not instantly — a late message may briefly appear out of place before settling into the agreed order.

