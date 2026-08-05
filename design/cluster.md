# Cluster

> This is the only part with genuine distributed risk. It is last in the ROADMAP and must come after the DST skeleton ([step 4](../ROADMAP.md#4-确定性模拟测试骨架)).

## Conclusion first: no consensus

`gor` has no Raft, no Paxos, no quorum logic of any kind. Linearizability is outsourced to a **shared table with CAS**.

This is not a compromise; it is copying a proven approach. Grep `quorum|consensus|Raft|Paxos` in Orleans' `MembershipService`: **zero hits** — it relies on a shared table plus ETag/CAS, heartbeat probing, and death votes with expiry. Measured details in [research/orleans-internals.md](../research/orleans-internals.md) (in Chinese).

The cost is explicit: **the shared table is a single point of failure.** While the table is unavailable, the cluster cannot change membership (existing nodes keep serving). This cost buys the removal of an entire consensus implementation; it is worth it.

## Membership

One table, one row per node:

```
member(node_addr, generation, status, iam_alive_at, suspect_votes, etag)
```

This is the shape after step 6c. `suspect_votes` is only written once probing and voting are enabled.

A cluster node must be configured with both the membership table and the transport; configuring only one of them is an invalid configuration. The membership table provides the shared membership view; the transport provides calls and direct probing to other members; a node missing either one does not join the cluster.

The primary key is (node_addr, generation). **`generation` is a fresh value taken at every node start**; after a restart, the same address gets a new row. Without it, a restarted node would claim the row of its previous incarnation, while others may still be casting death votes on that row.

### The table's interface

Three operations:

- **Write your own row** — CAS with the etag. Joining, heartbeat, and state changes all go through here. A zero etag means "this row must not exist yet", the same convention as the state table.
- **Write someone else's vote** — the current probe neighbor can CAS-update the target row's `suspect_votes` with the etag. Declaring death also writes the target row.
- **Read the whole table** — the node computes its view from it.

No convenience methods like "read one row" or "read only the live ones". The view must be computed from one full-table snapshot; reading in fragments lets a node see mutually contradictory pieces.

No "delete a row" either. Dead rows stay; they are how a restarted node recognizes its previous incarnation. Cleanup is an operations concern, not the runtime's.

One full-table read returns a `MemberSnapshot`: a single snapshot of the rows plus `TableNow`. `TableNow` comes from the injected `Clock` held by the membership table and shared by all clients. It is the only time base for vote expiry. A node's own `Clock` cannot be used to judge other nodes' votes, because node clocks can be offset.

Production code also takes time from this `Clock`. The membership table and the node code must not call `time.Now()` directly.

### The node state machine

```
joining → active → dead
```

**Joining**: write your own row as `joining` (zero etag), read the whole table, then CAS to `active`. The write-then-read order is deliberate: it guarantees that any node that can see itself is also seen by the other side.

**Heartbeat**: periodically CAS-update your own `iam_alive_at` against the table's `TableNow`.

**Leaving**: CAS yourself to `dead` on `Close()`. A node that leaves cleanly does not need others to declare it dead.

**Declaring death**: decided only by the current neighbors' unexpired `suspect_votes`. A stale `iam_alive_at` is not evidence of death.

`dead` is terminal. A declared-dead node must not modify its own row even if it is still alive — its CAS fails on the etag mismatch, and then it must self-terminate; it must not keep serving under an identity the whole world considers dead.

**But a CAS failure alone is not proof of your own death.** A heartbeat CAS collision has two causes: someone else changed the row to `dead`, or the previous heartbeat actually landed and only the reply was lost on the way — the latter advances the etag without the node knowing. Both causes give the same signal, and self-termination is irreversible, so on a collision the node must read the whole table again: if its row is `dead` it self-terminates; otherwise it takes the fresh etag and keeps heartbeating.

Self-terminating without reading would let one dropped packet kill a healthy node.

### The view is computed only from rows read from the table

**A table read failure is not evidence that anyone died.** If the table cannot be read this round, keep the previous view and change nothing. Degrading to "only me" would amplify one read fault into a whole-network re-sharding.

**Declaring death is write-then-believe.** A local count of enough valid votes does not enter the view until the CAS succeeds. If the write did not land, treat the other side as alive.

A failed direct probe does not remove the target from the view either. The prober first CAS-writes its vote; until the death-declaring CAS succeeds, the target stays `active`.

Death must go through the table; only then does the matter have an answer everyone agrees on.

### After self-termination, the runtime must also stop

"Must not modify your own row" is not enough. A declared-dead node still holds several activations whose ETags are stale, while new calls were already routed to other nodes.

So when a node sees its current generation as `dead` in a successfully read snapshot, it must report the cause "declared dead externally" to the root runtime. The root runtime first stops admitting entity calls and closes the public stop signal, then follows the abrupt stop: cancel executing calls, reject the queue, and drop activations. Calls after that return an error that the node has stopped serving. In the view a dead node computes, it owns nothing — but rejecting local calls cannot wait for the view to change; the old view may still assign some identity to it for a while.

An active `Close()` also writes the node's membership row as `dead`. That is only a normal leave — the root runtime already began a graceful stop — and the cluster node's completion signal must not be mistaken for an external death declaration. The cluster node must hand its end reason to the root runtime; a bare `Done` channel that carries no reason is not enough.

### Gap

The cluster node implements an explicit end reason: both an active close and an external death declaration close `Done()`, but only the external death declaration closes `DeclaredDead()`. The root runtime distinguishes the two by this channel, no longer inferring from "did I initiate the close myself". A declared-dead node no longer publishes the final empty view: in the view a dead node computes it owns nothing, but rejecting local calls takes effect immediately at the stop transition through the root admission gate, without waiting for the view to change, so this view must not be published (it would trigger graceful deactivation on view change, conflicting with the abrupt stop). This section is implemented.

**An embedding application must be able to know this.** The runtime provides a channel whose closing means "no longer serving"; `Close()`, `Kill()`, and being declared dead all close it. Without this signal, the application could only guess from every call erroring — and by then it is already serving a service that does not work.

A declared-dead node does not crawl back on its own. Coming back means re-joining: a fresh generation, a new row.

## Probing and death votes

Direct probing is the second information source for death judgment. A slow membership table cannot masquerade as probe success; one table read failure can no longer declare every node dead.

### Who probes whom

Probing uses a single-point membership ring, not the placement ring's virtual points. Each `active` `(node_addr, generation)` places exactly one point: `hash(node_addr + generation)`. Equal hashes order by the full member ID.

A node probes its clockwise and counter-clockwise neighbor members. When both directions point at the same member, it probes once. With one `active` member there is no target; with two, each probes the other once; with three or more, every node has two targets.

The ring is rebuilt on every successful read of a fresh member snapshot. New neighbors start counting failures at zero. In-flight probes to old neighbors are canceled, their failure counts are deleted, and their votes on the old neighbor's row are retracted by CAS. Same address with a different generation is also a new neighbor; it inherits no old count and no old vote.

This keeps the probe count per node constant at two. A bigger cluster does not increase any single node's probing load.

### The probe path

Probing reuses `Transport.Send` from [transport.md](transport.md). It goes through the same lazy dialing, framing, multiplexing, and fake-transport path. No separate UDP, HTTP, or side-channel sockets.

`cluster` does not import `transport`. It only depends on an async `Prober`: give it a target member ID, get back a reply channel. `gor`'s adapter sends the `probe` request defined in [Envelope](#envelope) via `Transport.Send`. The transport's server-side handler dispatches on `kind`; `probe` goes straight to `cluster`, not through an entity call.

A probe request carries only `kind`. The server's current member ID goes into the ordinary response's `reply`, and the initiator compares it with the target in its snapshot; only an exact match counts as success. A new process reusing the address must not erase votes for an old generation.

The server replies with its current member ID while its local member is still `active` and not stopping; otherwise it returns an error response with no member ID. Probing itself does not read or write the membership table, does not refresh heartbeats, and is not forwarded a second time.

The probe state machine waits on the reply channel, the close signal, and a timeout channel created from the `Clock`. A timeout cancels this `Send`. No `context.WithTimeout`, no wall clock.

### Failure thresholds

Each target has its own consecutive-failure count. Success resets it to zero. Timeout, dial failure, connection drop, and error reply each add one. A vote is written only when consecutive failures reach `ProbeFailures`.

Default parameters:

| Parameter | Default | Reason |
| --- | --- | --- |
| `ProbeInterval` | 1 s | Matches the existing heartbeat and view period. |
| `ProbeTimeout` | 500 ms | Shorter than one period, so unfinished probes do not pile up. |
| `ProbeFailures` | 3 | One or two dropped packets do not vote; the judgment time stays around the old three-second window. |
| `VoteTTL` | 6 s | Twice the three-probe window; two neighbors can converge, and old votes expire quickly. |
| `MaxTickGap` | 2 s | Beyond two probe periods, the node can no longer judge consecutive failures reliably. |
| `MaxTableLatency` | 500 ms | When the node's own membership-table access is slower than one probe, it must not declare others dead. |

All of these live in `cluster.Config`. `VoteTTL` is six times `ProbeInterval`, `MaxTickGap` twice; `ProbeTimeout` and `MaxTableLatency` are each half.

### Votes

`suspect_votes` is a map deduplicated by voter member ID:

```go
map[MemberID]SuspectVote

type SuspectVote struct {
	ExpiresAt time.Time
}
```

`MemberID` is `(node_addr, generation)`. Once the failure threshold is reached, the prober writes its own entry as `ExpiresAt = TableNow + VoteTTL`. Later failures only extend its own entry. A successful probe immediately retracts its own entry.

Only the target's current forward and backward neighbors may create or renew votes. Any former voter may retract its own entry. Votes from the target itself, from non-neighbors, and from `joining`/`dead` members are invalid. A valid vote must also be unexpired: `ExpiresAt > TableNow`.

Writing a vote is a CAS on the target row. The writer takes the target row from a full snapshot, deletes expired entries, merges its own change, and commits with the row's etag. On a CAS conflict it re-reads the full snapshot and recomputes; it must not write back the whole old `suspect_votes` column, dropping concurrent votes. The target's own heartbeat also preserves unexpired votes and cleans expired ones when writing.

Reads do not clean. Expired entries are cleared on the next write to the row. Death counts are always filtered by `TableNow`, so leftover old data has no effect.

### Vote counts and declaring death

For an `active` target row, recompute the probe ring on the same member snapshot first. Count only the valid votes of the target's current forward and backward neighbors.

With `n` active members, the required vote count is `min(2, n-1)`:

- `n == 1`: no external evidence; cannot be voted dead.
- `n == 2`: one vote from the only neighbor suffices to declare death.
- `n >= 3`: both current neighbors must vote.

Only an `active` node that is locally self-check healthy may CAS the target to `dead` on sufficient votes. It keeps the target row's valid votes and uses the current etag. On a CAS conflict it re-reads the table; if the target is already `dead` it is done, and only if it is still `active` does it recompute.

The threshold is not a majority of all members. Only the two neighbors probe this target directly; demanding more votes would let nodes without probing duty decide death, and bring per-node work back to a shape that grows with the cluster.

### Symmetric partitions

When a transport partition does not affect the membership table, the table's final state depends on how the two groups sit on the probe ring. With each group contiguous, every boundary node has only one cross-partition vote, short of the two-vote threshold. With the groups interleaved, every node's two neighbors are on the other side; four nodes split 2+2 can CAS all four rows to `dead`.

This is an accepted cost. The membership table has no connectivity topology and no consensus; it cannot distinguish "the other side is dead" from "the other side is alive but unreachable". There is no floor rule of "the last `active` member must not die". Such a rule would only let CAS timing pick an arbitrary survivor; it cannot restore connectivity, nor prove the survivor more trustworthy than the node voted dead.

At `n == 1` no new death votes are produced, but this is not a global floor: while two nodes are both still `active`, they can simultaneously CAS each other to death, and the table can still end up with zero `active` rows.

After total death, the membership table holds only `dead` rows, every node's `Done()` is closed, and the membership view is empty. A node still running that sees the empty view returns the no-owner error for calls and never falls back to local execution; a node already declared dead returns the stopped-serving error first. Nodes do not resurrect themselves.

Recovery requires operations to start at least one node that joins the same membership table with a fresh generation. It restores the membership view after writing a new `joining`/`active` row; old `dead` rows stay. Other nodes join the same way with fresh generations. There is no automatic recovery, and changing an old row back to `active` is not allowed.

### Self-check

Every probe period, the node checks three things:

- The local `Clock` interval from the previous tick to this one. Going backward or exceeding `MaxTickGap` means a GC pause, a scheduling stall, or a clock jump.
- The completion time of one full-table read.
- The completion time of one CAS on its own membership row.

If either of the last two exceeds `MaxTableLatency`, or does not finish before its `Clock` timeout, the node enters `unhealthy`. A static clock offset does not affect the first item; only two consecutive readings of the same node are compared.

While `unhealthy`, the node keeps reading the table, heartbeating, and probing, but clears failure counts, writes no new votes and renews none, and declares no deaths from votes. Existing votes expire naturally by TTL. A successful probe can still retract its own vote.

After three full periods with none of the above anomalies, the node returns to `healthy`. Three beats keep a brief recovery from immediately regaining voting rights.

A failed self-check is not a reason to self-terminate. A table read failure still keeps the old view; only seeing its own current generation as `dead` in a successfully read snapshot stops the node. A node whose membership-table access merely slowed down thus does not amplify the fault in turn.

### A node voted dead

Every successful table read, and every re-read after a heartbeat CAS collision, checks the node's own `(node_addr, generation)`. If the state is already `dead`, it immediately takes the existing stop path: drop all activations, reject later calls, and close `Done()`. After that it no longer answers probes.

So a node whose transport is cut off but whose process is healthy sees the `dead` its neighbors voted while it can still reach the membership table, and stops itself. It cannot resurrect with one later probe success; resurrection means re-joining with a fresh generation.

### The place of `iam_alive_at`

`iam_alive_at` stays and keeps being updated by heartbeats. It is the last table heartbeat visible to operations, not evidence of death.

Step 6c deletes all "CAS to `dead` when stale beyond `DeadAfter`" logic. Table read failures, old timestamps, and missing heartbeats cannot replace direct probing and valid votes.

### Acceptance

All of the following are verified in `make sim` with the fake transport and the fake membership table:

- Three nodes share the membership table; one node's transport is partitioned without stopping its process or its table access. The two neighbors each write a vote, the target row becomes `dead`, and the partitioned node closes `Done()`.
- One neighbor leaves a vote after flapping and stops renewing it. Table time passes `VoteTTL`; the other neighbor then also suffers three failures. A healthy target must not be declared dead on one expired vote plus one fresh vote.
- Freeze the target's `iam_alive_at` while direct probes keep succeeding. The target stays `active`.
- Inject local clock jumps, GC pauses, and a slow membership table. The node writes no votes, renews none, and declares no deaths from existing votes; it only regains voting after three consecutive healthy beats.
- Four nodes interleave 2+2 on the probe ring while the membership table stays available. All four rows can become `dead` and every `Done()` closes; starting one node with a fresh generation brings `active` members back into the view.

## Placement

A consistent-hash ring: nodes hash onto the ring by address, and entities land on the first `active` node by hashing their Identity.

A hash ring is chosen over "random placement plus directory lookup": **a hash ring makes locating mostly pure local computation, with no network round trip.** The cost is that node changes cause activation migration.

No load-based placement. It needs a global load view, and a global view is extremely hard to verify in DST. The scale target is small clusters; a hash ring is enough.

### Hashing must be stable across processes

**`maphash` cannot be used.** It has a random seed per process; two nodes would compute two rings, and a converged view would not help. Use a fixed function written into the code: `hash/fnv` is enough.

**Each node places several virtual points on the ring, not one.** With three to five nodes at one point each, the distribution skews badly and one node carries half the keys. The virtual-point count is a constant, not a configuration knob — it is not something users should tune.

A virtual point's position comes from `hash(address + generation + index)`. Including the generation lets a node restarted at the same address land on new positions instead of inheriting its previous incarnation's load distribution.

### The ring is the directory; there is no second table

Placement is computed from `hash(Identity)` plus the current membership view — one pure local computation. **No separate directory table records "who is where".**

Orleans has a directory table because it does not place by hash: it puts activations on chosen silos and uses the ring only to partition the directory, so something must keep the books. `gor` places by ring; the ring itself is the ledger.

A directory table would narrow the double-activation window: two nodes with inconsistent views each register, and CAS makes one lose. But it cannot narrow the window to zero — the loser already activated — and the cost is one more round trip per activation, one more table, and one more degradation path when the table is unavailable. The window must be acknowledged in the docs anyway; narrowing it is not worth that price.

### When the view changes, drop activations that are no longer yours

After a membership view change, some activations on the node no longer hash to it. **These activations are dropped on the spot**, not left to idle eviction.

Keeping them has only downsides: they hold stale ETags, so the next write must hit a conflict; and new calls are already routed to the new node, so they will never be served. The drop takes the same path as idle eviction; no new path is added.

## Directory consistency — the part that must be honest

**`gor` does not guarantee a single activation world-wide at any moment.**

While nodes join or leave, nodes' views of the member list briefly disagree, so the same key can be computed to different nodes, producing two activations. The window closes once the membership views converge.

Rejecting the directory table above is admitting this window cannot be closed: an arbitration layer only narrows it, and the code users write is the same whether the window is narrow or not. So:

1. The docs state this window where users can see it.
2. State writes are forced through the ETag ([persistence.md](persistence.md)); double activations collide instead of overwriting each other.
3. DST scenarios cover the double-activation window specifically, asserting "no silent write loss" rather than "no double activation".

**This is Orleans' semantics as-is**; its official docs say exactly this. Pretending to do better only makes users write code on false assumptions.

## Routing

Every call first computes which node `hash(Identity)` lands on:

- **Self** — hand to `runtime` as usual, exactly as in single-process mode.
- **Someone else** — forward it (transport in the next section).

**This step happens in `gor`, not in `runtime`.** `runtime` is about "calls on the same key are serialized"; it must not know the cluster exists, just as it does not know `store` exists today. Ring computation and forwarding both happen in the `gor` layer; `runtime`'s interface does not change.

The ring and the membership view get their own package, shaped like `timer`: it takes a membership-table interface, a `Clock`, and its own address, and periodically reads the full table against the injected clock to compute the view. `gor` wires it up and, on view changes, hands the activations that no longer belong to this node to `runtime` for dropping.

**The ring is a pure function.** Give it a membership view and an Identity, and it computes a node. It reads no time, does no I/O, holds no state; unit tests feed it views directly. Fetching the view is the stateful half, kept separate from the ring.

## Transport

One long-lived connection between nodes, requests multiplexed, no gRPC. Interface, frame format, and connection lifecycle in [transport.md](transport.md).

`cluster` does not import `transport` — the ring computes an address, and forwarding is initiated by the `gor` layer.

## Forwarding

The transport moves opaque bytes; all encoding and decoding lives in the `gor` layer.

### Envelope

Every request carries a `kind`. Step 6c introduces this field. Responses carry the return value and a structured error envelope, defined in [errors.md](errors.md).

```
invoke  {"kind": "invoke", "type": ..., "key": ..., "method": ..., "args": ...}
probe   {"kind": "probe"}
reply   {"reply": ..., "error": {"code": ..., "message": ...}}
```

`invoke`'s `args` and `reply` are raw JSON. Only generated code knows what types they should decode into; see [codegen.md](codegen.md). `probe` has no `args`; its `reply` is the current member ID as JSON.

The server accepts only `invoke` and `probe`. An unknown `kind` returns `gor.invalid_request`. There is no special meaning for an empty `type` or missing fields.

Step 6b's forwarding has only the `invoke` shape. Step 6c adds `kind` to the same request envelope; it does not add a second response format.

**A method's error travels in the response's field, not in the transport's error frame.** The error frame is the transport's own fault — connection dropped, frame too long. Mixed together, the caller could not tell "not delivered" from "delivered, but the method returned an error". The response's error envelope keeps the stable code and the diagnostic text; it does not carry the error object. The rules and the local parity scope are in [errors.md](errors.md).

### Not forwarded a second time

A node that receives a forwarded call executes it directly, **without computing ownership again**. Its view may differ from the caller's, and recomputing has only two outcomes: bouncing back (a loop waiting to happen) or an error the caller cannot handle.

Not computing ownership is not asking nothing: **a node that is already `dead` or stopping rejects all forwarded calls.** This is a rule about itself; it needs no view, so it cannot fight with anyone else's view.

When views disagree, two nodes each activate the same key — that is the double-activation window acknowledged above, held off by the ETag against concurrent writes, not by this rule.

### Cancellation does not cross the network

When the caller's ctx is canceled, the forwarding side drops the pending request and returns `ctx.Err()`; the method on the other side keeps running to completion. The server-side context carries no caller cancellation or deadline. Full rules in [errors.md](errors.md).

### Forwarding does not retry

Cannot send, connection dropped, the other side rejected — the error goes straight to the caller. Only the user knows whether retrying is safe; this is the same stance as with `State.Set()` conflicts and scheduled delivery failures.

## Migration

When a node leaves, its activations disappear; requests routed to the new node reactivate them from the store.

**No hot state migration.** State is in the store anyway; rebuilding from the store is the same path, and an extra migration path is a pile of code that only runs on failure.

## Rolling upgrades

No-downtime upgrades with incompatible changes are not supported; the reason is in [architecture.md](architecture.md): no version-tolerant encoding.

The same cluster is assumed to run the same version of the binary. This is a capability the project explicitly gives up relative to Orleans.

## Gap

The current implementation covers this section's membership snapshots, probing, votes with expiry, self-checks, and the node self-termination path. Still uncovered: operational cleanup not listed in step 6c, and rolling-upgrade capability.
