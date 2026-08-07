# The simulation test skeleton

[testing.md](testing.md) says what simulation tests should look like and why it is an architectural constraint. This document says exactly which pieces the skeleton is made of.

## What this step builds

Skeleton = seed + fault injection + crash and restart + event log + invariant assertions.

**No real network.** Cross-node faults are injected through the fake `Transport` under the `sim` build tag; it hangs a new fault source onto the existing skeleton, not making tests depend on a real network.

This is what "DST before the cluster" actually means: build the driver and the assertions first; when the cluster arrives, it plugs straight in.

## Where determinism ends

Goroutine scheduling cannot be controlled in Go. This decides what the skeleton can and cannot promise, and it is the most important section of this document.

Two kinds of nondeterminism must be kept apart:

- **Injected** — what the next step does, whether this read or write fails, how long the delay is. All decided by one PRNG driven by the seed, drawn only in the single driver goroutine. Fully reproducible.
- **Environmental** — which of two concurrent calls enters the mailbox first, which goroutine wakes up first. Not controllable.

A step looks like this:

1. Draw this step's actions and fault configuration from the PRNG.
2. Execute — start some call goroutines.
3. `synctest.Wait()` until the bubble is quiescent.
4. Observe at the quiescence point and check invariants.

## The log splits in two

This is the easiest place to get wrong; it is worth stating in no uncertain terms.

Intuitively, observations at the quiescence point should be interleaving-independent: all calls are done, `Add` is commutative, and the entity's value is the same regardless of order.

**Once faults can deactivate activations, this stops holding.** Two concurrent calls on the same entity with "the write took effect but errored" injected: the first call writes, errors, and the activation is deactivated. The second call either is still queued and gets rejected along with it (the entity's value was incremented once), or re-activates in time, reads the new value, and writes again (incremented twice). Which one happens depends on scheduling.

The multiset of outcomes is the same: both `{write error, write error}` and `{write error, closed}` are possible.

So: **with fault injection, no observation is interleaving-independent.** The log therefore splits in two:

- **Decisions** — what the PRNG produced: which entity this step hits, how much to add, which fault to inject, whether to crash. Fully decided by the seed.
- **Observations** — outcomes, entity values, intermediate quantities of invariant checks. For humans; when something fails, reconstruct the incident from them.

**The reproduction test compares only the decision half.** The observation half is still written to the log, but does not take part in the comparison.

This is not lowering the bar. What reproduction must guarantee is "re-running with the seed hits the same fault sequence again", and that is fully decided by the decision half. Decision reproducibility is not tested for nothing: it guards against the skeleton's own classic bug, drawing the PRNG from several goroutines. Once the draw order scrambles, the seed never reproduces anything again.

**Do not shrink the load to make observations comparable.** Making concurrency serial, making delta 0, or excluding write faults from the random pool all make the log deterministic, at the cost of the thing under test being gone.

Interleaving-dependent correctness does not rely on reproduction; it relies on two things: invariant assertions (must hold for every interleaving) and porcupine (checks whether a history linearizes, which is interleaving-independent by nature).

Go offers nothing stronger ([testing.md](testing.md) cites Resonate's same conclusion). Writing the impossible as possible only makes nobody know what to trust when the first flake appears.

## A fault names its target

*The log splits in two* makes one promise about reproducibility: re-running with the seed yields byte-identical **decision** lines. For that to hold, the decision half must be a pure function of the seed — nothing the goroutine scheduler can change may flow into it. Tracing exactly where that line was crossed is the whole of this question.

The decision encoding reads node liveness — `liveNodeIDs()`, `stoppedNodeIDs()` — to pick valid targets: which node to crash, to call, to restart. That read is **necessary**; the driver cannot pick a live node without knowing which are live, and every action case depends on it. It is **safe** on one condition: the liveness it reads must itself be seed-determined. Crash and leave are decisions, so they move liveness deterministically. Restart is a decision to *attempt*; whether the attempt succeeds is an observation, pinned to the seed by the remedy below. Those three are the movers this section owns. A fourth mover — the cluster declaring a node dead by vote — is an observation that is *not* seed-determined; it has its own section, *Liveness has two sources*, because its remedy is at the read rather than at a fault.

A one-shot fault with no target breaks the condition. The member fault is a single field consumed by whichever `WriteMember` or `ListMembers` takes the lock first. During a restart the new runtime's join write and a survivor's heartbeat write both reach the member store while such a fault is pending; they write **different rows**, both can succeed, and whichever the scheduler runs first consumes the fault. The fault is **drawn** deterministically and **applied** nondeterministically. Its application fixes the join write's fate, which fixes restart-success — an observation — which moves liveness, which the decision encoding reads next step. A divergence the split permits on the observation side (restart outcome) walks through a necessary read into the decision side (which node the next step targets), and the decision lines split from the next step on.

That is the defect, and it is **one root, not two**. "May a fault be consumed nondeterministically" and "may the decision encoding read runtime state" are the same leak seen from two ends: the fault's first-arrival consumption is what makes the runtime state scheduler-dependent, and reading that state is what carries the dependence into the decision half. Fixing the read is not an option — the read is necessary, and the state is scheduler-dependent only because of the fault. The fix is at the fault.

### The remedy: bind the target by the seed

A fault is two facts — a kind and a target. The kind has always been a decision. The target must be a decision too. The store fault already does this: keyed by entity identity, drawn in the driver, read fresh on every call to that entity, not consumed by first arrival. The member fault must meet the same bar. This is not new machinery; it is removing the inconsistency that left the member fault the odd one out.

The target of a member fault is a member row — the `(node address, generation)` the member store keys on — for the write and delay kinds, and a node for the list-error kind. The driver draws the target with the seed, the same way it draws node indices for calls and crashes, resolving a node to its current generation. The fault fires only on an operation addressing that target; if none does this step it does not fire — dropped, deterministically, the way a store fault on an entity nobody calls does not manifest. The delay kind is already shape-bound to an active-refresh write; it takes the target row as well, so two survivors heartbeating no longer race for one delay token. With the target fixed by the seed, restart-success is fixed by the seed (a write or list fault fails restart exactly when it targets the restarting node; a delay never does), liveness is fixed by the seed, and the decision half is pure again.

### Per seam

Whether a seam carries this defect turns on one test: does its first-arrival consumption move a runtime quantity the decision encoding reads?

- **Store read/write fault** — keyed by entity identity, not consumed by first arrival. No defect.
- **Schedule claim fault** — keyed by entity identity; the consuming `Claim` is CAS-unique, so the fault rides the one winner. Which node wins is scheduling, but the observable — one delivery, the fault applied — is invariant, and liveness is untouched. No defect; the residual scheduling dependence is the accepted outcome kind.
- **Member fault** — single unkeyed field, first-arrival. Its consumption fixes restart-success, which moves liveness, which the decision encoding reads. **The defect.**
- **Schedule list fault** — single unkeyed field, first-arrival, same *shape* as the member fault. But a list error is read-only and the poller retries next tick; it moves no quantity the decision encoding reads, so no divergence reaches the decision half. **Benign today; take the same target binding for consistency, not urgency** — a future seam that let schedule state feed a decision would reopen the leak through the same shape.
- **Network fault** — not this shape. A partition is a deterministic group map applied per node pair (a whole pair goes silent); a per-message drop is drawn by the seed in the driver; delay is drawn unconditionally in the driver and released by the clock. None latches onto a target by first arrival.

### What does not change

Concurrency is not reduced and no fault is removed. Binding a target is not "shrinking the load to make observations comparable": calls stay concurrent, faults still fire, and the outcome of a fault on its bound target is still interleaving-dependent. The member fault's *kind* was always a decision; only its *target* was left to the scheduler, and only the target moves onto the seed.

The invariants do not change. Membership views still converge, declaring death is still irreversible, and one owner per key still holds after convergence — these hold whichever target a fault hits, and binding the target does not narrow them.

The reproduction test does not change in shape. It still compares decision lines. A fault's target is now part of the decision line, so it is part of what is compared — correct, because the target is now a decision.

## Liveness has two sources; only one may feed the decision

The liveness the decision encoding reads must be a pure function of the seed. A node stops being live in two unrelated ways, and only one of them qualifies.

- **Driver liveness.** The driver crashed the node, left it, or its restart failed. Crash and leave are decisions; restart-success is an observation pinned to the seed by *A fault names its target*. This liveness is a pure function of the seed.
- **Cluster liveness.** Probes time out, neighbours vote, the vote clears the threshold, and the runtime collapses itself to dead (`becomeDead`, stop code `ErrNodeDead`). Which probe `select` wins when several are ready is scheduling. This is an **observation** by design — the probe loop is supposed to be scheduling-dependent; making it deterministic would defeat the test.

Both close the same channel (`rt.Done()`), so one `runtimeStopped` check cannot tell them apart. The decision encoding must not read that one check through `liveNodeIDs()`, `stoppedNodeIDs()`, `targetPool()`, or the action choice — it would read both. An observation that moves the decision half breaks reproduction. This is the member-fault leak from the read end: same defect, a different door.

### The read, not the source

*A fault names its target* remedies the member-fault leak at the source: the driver owns the fault, and the seed binds its target. That remedy does not exist here — the source of cluster liveness is the production probe/vote mechanism, and it is *meant* to be scheduling-dependent. The source stays; the decision encoding reads a liveness set the driver owns — the nodes it has crashed, left, or failed to restart — and nothing else. The cluster's `becomeDead` keeps closing `rt.Done()`; observations and invariants keep reading cluster liveness. The two notions do not have to agree, and after a vote they routinely will not.

### What disagreement costs, and buys

When the driver thinks a node is live and the cluster has declared it dead, the driver still targets it. A call returns `ErrNodeDead` at admission; a crash is a no-op on an already-dead root; a restart closes and rebuilds it. None of this breaks an invariant — `ErrNodeDead` is a clean rejection and the store is untouched — and it is exactly the state the cluster exists to test: two views of who is alive, disagreeing. Reading driver liveness presses on that window; a decision fed by cluster liveness would fold to the cluster's view the instant a vote clears and never reach it.

The cost is narrow. The fault target pool grows by the cluster-dead-but-driver-live nodes, and a fault drawn against such a node may not fire — the node rejects at admission before the fault injection point. This lowers the trigger rate, but only inside the disagreement window. Outside it the two liveness notions coincide and the rate is unchanged. If the rate regresses past what *A fault names its target* fought to reach, the answer is more seeds, not re-conflating the read.

### What stays

The probe loop, the vote, and `becomeDead` are production code; the skeleton does not touch them. Concurrency is not reduced. The reproduction test compares decision lines.

The decision side and the invariant side each have their own liveness reader. `checkInvariants` reads cluster liveness — owner uniqueness is an observation. The driver-owned liveness feeds only the decision side: feeding the invariant side would make a cluster-dead-but-driver-live node read its stale last view and raise a false owner-uniqueness red.

## What a "node" is

A node = one `runtime.Runtime`. Several nodes share one `Store`. Before [step 6](../ROADMAP.md#6-multiple-nodes), nodes have no other connection.

- **Crash** — drop all in-memory state, keep the store.
- **Restart** — build a new `Runtime` on the same store.

**Double activation becomes testable here.** Two Runtimes sharing one store activating the same identity is double activation by itself — no network partition needed to produce it. The core risk of cluster instability is already covered by assertions at step 4; step 6 only changes the way it is produced.

## A crash is not Close

`Close()` drains the mailbox and waits for in-flight calls to finish. That is a graceful stop.

A crash must make in-flight calls return with an error immediately, giving entities no teardown chance. So `runtime` needs one more stop path: `Kill()` — cancel all in-flight calls' contexts, close the mailbox, do not wait for draining.

**`Kill()` must make every goroutine exit.** This is not cleanliness: synctest panics with a deadlock report when every goroutine in the bubble blocks forever. A leaking crashed node does not leak quietly; it takes down the whole simulation test.

Go cannot kill a call that is executing a user method. `Kill()` can only cancel the context; a user method that ignores its context keeps running to completion. This differs from a real process crash, but there is no other way, and the entities in simulation tests are written by us.

**After a crash, wait for the fake store to finish its in-flight work.** `Kill()` cancels the context and the caller returns with the cancellation error right away, but the entity method is still asleep inside the fake store. The bubble's fake clock only advances while every goroutine is durably blocked, and once the root exits it stops outright: the sleeping goroutine can never wake, and synctest reports a leak.

So the fake store must be able to report "no work in hand", and the driver waits for it before every observation step. The waiting is done with a channel: once the driver blocks, the fake clock advances, and the sleep ends on its own.

**Wherever time is advanced, wait for it — not only before observations.** The bubble's clock only moves while every goroutine is durably blocked, and the moment `synctest.Wait()` returns, the driver itself is running again: as long as the driver does not block, injected delays never move a step.

Waiting only before observations does not cost "a bit of slowness"; a component can stay stuck in one injected delay forever: its loop never turns again, yet it looks alive. This symptom disguises itself as a bug in the system under test — last time it disguised itself as "two nodes each computing a view that contains only itself", and it took a long investigation to find the driver.

A side benefit: writes the crashed node never got to answer land in this step's observation — exactly as in the real world. Step 6's fake network will hit the very same problem; the interface is settled here first.

## The fault injection seam

The `Store` is the storage-side I/O seam. (The cross-node `Transport` seam is its own subject below, in *The fake network's fault classes*.) The fake store implements `store.Store` and injects four kinds by seed:

- Read failure
- Write failure that did not take effect
- **Write failure that took effect**
- Delay (`time.Sleep`, fake inside the bubble)

The third is the point. It landed, the reply was lost, the caller does not know — the most common and most miswritten kind of outcome in distributed storage. [persistence.md](persistence.md)'s "after a failed write" rule exists precisely for it, and the simulation tests must prove it sufficient.

Slow responses are not a separate kind; they are the delay.

## The fake network's fault classes

The fake `Transport` is the cross-node fault seam. Four things get conflated under "network fault"; only some are real, distinct faults under this transport model.

**Partition** — a set of node pairs that cannot talk. Implemented as a group map; a `Send` across groups returns a partition error at once. Deterministic: the partition is a driver decision, not a coin flip per message. This is the current capability.

**Drop** — a message that never arrives. A per-message drop keyed to the seed is the same shape; partition covers the coarse form (a whole pair goes silent).

**Delay** — a message held for a seed-drawn number of fake-clock ticks before delivery. This is the only timing fault that matters under this transport model. Determinism rests on one rule: **the delay is fake-clock time, and delivery is released by the bubble's clock, not by a goroutine sleep the scheduler happens to honor.** The message sits in a queue and a clock timer fires to deliver it, exactly as the Store delay already works (`time.Sleep`, fake inside the bubble); `synctest.Wait()` advances the clock because the wait is durably blocking — a timer channel, not a mutex ([testing.md](testing.md) rule 4). The delay value is drawn from the seeded PRNG in the single driver goroutine, like every other fault. The same quiescence discipline the Store obeys applies: wherever the clock can advance, the driver waits for delayed deliveries to settle before observing, or a component stalls inside one delay forever and looks alive.

**Reorder is not a distinct fault here.** A reorder knob sounds like the obvious fourth, but the transport model dissolves it:

- *Within one connection*, replies already return out of order relative to requests — handlers finish at different times, and the pending table matches by correlation id ([transport.md](transport.md)). Reordered replies are the transport's normal mode, not a fault; reordering request *bytes* within one connection would simulate something TCP does not do, since one stream delivers bytes in order.
- *Across connections*, "which of two independent messages lands first" is just two independent delays. There is no shared queue to reorder against.

So the meaningful timing fault is delay, and delay produces every cross-message reorder for free. Building a separate reorder mechanism would simulate non-TCP behavior.

## What network injection cannot catch

Network fault injection controls **inter-node** message timing: when a message, once handed to the transport, is delivered to the recipient. It does not control **intra-node** goroutine scheduling: which of two goroutines on the same node runs first. [testing.md](testing.md) states the boundary plainly — synctest is a unit-testing tool, not a DST framework, and does not control goroutine scheduling order.

The consequence is sharp. A bug whose window is "node A tears itself down before it finishes a local step" is a scheduling race on one node. Delaying or reordering the network message downstream of that step does not move the window — the close still races the local write regardless of when the recipient sees the result. Network injection cannot reach this class.

What does catch it: a **contract test with a blocking seam** at the teardown boundary, plus the **`-race` detector** as the statistical net. The blocking seam makes the window unavoidable — the test owns a channel that gates the in-flight work, so close cannot complete until the test releases it, and a broken close fails every run, not one in ten. This is how the graceful-close flush invariant is verified ([transport.md](transport.md) Testing); it is the pattern for teardown-ordering bugs generally, not a gap network injection could fill.

## Why inject a Clock at all

Inside the bubble, `time.Now()` is already fake, and `clock.Real` works as-is. So is the `Clock` interface redundant?

No, for two reasons:

- Unit tests outside the bubble also need to control time, and there `time.Now()` is real.
- One bubble has exactly one clock. To test clock skew between nodes at step 6, each node needs a `Clock` with an offset.

"No `time.Now()` in production code" stays. It guarantees time comes from a replaceable place; it does not guarantee any particular replacement.

## Invariants

Checked after every step. The first two are read directly from the fake store's bookkeeping, independent of entity type:

- **ETags are monotonic.** A record's ETag only grows.
- **Nothing is invented.** The content of storage at any moment must equal the bytes some `Write` committed. No write can be silently rewritten into something else.

The third needs a porcupine model:

- **The call history of a single entity is linearizable.**

## How porcupine plugs in

The entity under test is a counter: `Add(ctx, n) (int64, error)`, returning the value after the add. The sequential spec fits in three lines, while the interleavings are many — just right.

Each operation in the history records call time, return time, input, and output; times come from the bubble's fake clock.

**Calls with unknown outcomes are recorded as in-flight operations**, not as failures. After a crash or "the write took effect but errored", the caller gets an error, but the operation may have taken effect. For an operation that never returned, porcupine allows linearizing it at any position or not at all — which is exactly the accurate expression of "not knowing". Recording it as a failure makes the checker reject a history that is actually legal.

## The event log

One event per line, plain text, human-readable. Decisions and observations have their own prefixes; comparison filters by prefix:

```
seed=8f3c2a1b
0000 decision entity=Counter/a deltas=[3,5] fault=write.applied-then-error
     observe outcomes=[store-write-applied-then-error,closed]
     observe state Counter/a=3
0001 decision crash node=1
```

**Only decision lines are numbered, counting consecutively from 0.** Observation lines are not numbered. If numbering ran across both halves, what a decision line looks like would depend on how many observations came before it: the compared half would depend on the uncompared half, and a change in observation count would fail the reproduction test for reasons unrelated to the seed.

Re-running with the same seed gives byte-identical `decision` lines — this rule itself needs a test, and the test must cover the seed population, not one seed (see *The reproducibility gate covers the batch*).

**Any failure prints the whole log**, not just the decision half. The observation half exists for exactly this moment.

No JSON. When something fails, a human stares at two logs looking for the first differing line; `diff` beats everything.

## Package and tests

The `sim/` package, build tag `sim`, tests named with a `TestSim` prefix (`make sim` filters with `-run TestSim`).

`sim` depends on `gor`, `runtime`, `store`, and `clock`; none of them depend on it.

**Invariants run over a batch of seeds, not one.** One seed walks one trajectory and cannot cover several fault combinations. The seed list is hardcoded (for example 64 consecutive seeds from some base), so failures reproduce without introducing wall-clock randomness. Reproducibility has two guards, for two distinct bug classes. A single fixed seed, run twice, catches the skeleton's classic bug — drawing the PRNG from several goroutines — because that breaks *every* seed. It does not catch a leak that breaks only *some* seeds; for that, every seed the batch runs is run twice and its decision lines compared. The single-seed test is the smoke alarm; the batch is the contract.

porcupine (`github.com/anishathalye/porcupine`) is a new dependency.

## The reproducibility gate covers the batch, not one seed

*The log splits in two* promises byte-identical decision lines for **any** seed. A test that replays one seed verifies that promise for one seed — no more. Two bug classes hide behind that gap.

The first is the skeleton drawing the PRNG from several goroutines. It scrambles the draw order, so *every* seed stops reproducing; one replayed seed catches it. This is the class the single-seed test was built for, and it is enough for this class alone.

The second is a leak that breaks only *some* seeds (the liveness leak is one). A one-seed gate can sit inside the safe majority forever — green while the contract is broken. A batch that runs each seed once but never compares a seed against itself is just as blind: it walks many trajectories and checks none for replay fidelity.

The gate matches the contract: every seed the batch runs is run twice and its decision lines compared, alongside the coverage check. The single-seed replay is the smoke alarm for the PRNG-draw class, not the reproducibility gate.

### Cost

Running each seed twice doubles the batch's wall-clock — about 2.6s at a 64-seed window, out of the default `test` target and small. The only honest lever for cost is the window size, never skipping replay on some seeds: a gate that replays a subset reintroduces the same hole, just smaller.

One wrinkle, faced honestly. Replay divergence is itself scheduling-dependent, so *which* seeds diverge wobbles run to run, and a fragile seed can pass a given pair by luck. Over a population, at least one fragile seed fires on essentially every run while the contract is broken, so the gate is reliably red then and reliably green while the contract holds. The wobble does not rescue a broken contract; it only blurs the exact count.

## What the later steps hang on the skeleton

Step 5: the scheduled-task table is a new fault source (scan failures, claim failures, the claim landed but the reply was lost), with a new invariant: "one delivery per due time". See [timers.md](timers.md).

Step 6a: the membership table is yet another fault source, shaped like the scheduled-task table. New invariants:

- **Membership views eventually converge.** After faults stop, all live nodes compute the same view.
- **After convergence, one key belongs to one node.** During convergence there may be more than one: that is the acknowledged double-activation window, not a bug.
- **Declaring death is irreversible.** A declared-dead node does not crawl back to `active` by itself.

"Live nodes" means nodes that **still consider themselves alive**, not nodes the driver did not crash. A persistently slow membership table makes nodes declare each other dead until everyone self-terminates (see [cluster.md](cluster.md)); that is 6a's known failure mode, and with no owner left at all it must not count as a broken invariant. After everyone is dead, the restart action brings nodes back: a fresh generation, a new row, and convergence must still happen.

**Owner uniqueness must be checked with a batch of probe identities**, not just the two under test. `Owns` is pure computation; it writes nothing to storage and activates nothing, so a few dozen keys cost nothing, and checking a full batch equals comparing views: whenever two nodes' views differ, some key's owner necessarily disagrees. This also avoids opening a "hand over the view" method on the runtime for tests.

**After ownership filtering, `claim-lost` is no longer an event every seed batch hits.** A non-owner poller never claims; two pollers claiming the same row is only possible inside the inconsistent-view window. This is the result of [timers.md](timers.md)'s rule, not a coverage regression.

Step 6b:

- The fake `Transport` implementation: network partitions, dropping, and delay (the meaningful timing fault; reorder is not distinct — see *The fake network's fault classes*).
- One `Clock` with an offset per node.
- Concurrent writes during a partition are blocked by the ETag — this is not a new invariant; it is step 4's invariant re-run under a new fault source.

Step 6c: probe failures and voting; the new invariant is "a healthy node is not killed by expired votes".

## Gap

The fake network deterministically simulates partitions, drops, recovery, and delay.

**Reorder is no longer a goal.** Earlier text listed reorder as a fake-network capability; that was wrong for the reasons in *The fake network's fault classes*. Within a connection, out-of-order replies are the transport's normal correlation-id mode and a TCP stream is ordered; across connections, reorder is just independent delays.

The driver runs a fixed batch of 64 consecutive seeds (`simulationSeed` + 0..63), each walking one deterministic *decision* trajectory. It is not a seed search: there is no fuzzing over seeds at test time, and a search would not help the scheduling-race class anyway — the same seed can pass or fail by scheduling, since observations are interleaving-dependent by design (*The log splits in two*). What a search would broaden is decision-sequence coverage, a separate axis from fault breadth.
