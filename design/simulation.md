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

## What a "node" is

A node = one `runtime.Runtime`. Several nodes share one `Store`. Before [step 6](../ROADMAP.md#6-多节点), nodes have no other connection.

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

Right now there is exactly one real external I/O seam: `Store`. The fake store implements `store.Store` and injects four kinds by seed:

- Read failure
- Write failure that did not take effect
- **Write failure that took effect**
- Delay (`time.Sleep`, fake inside the bubble)

The third is the point. It landed, the reply was lost, the caller does not know — the most common and most miswritten kind of outcome in distributed storage. [persistence.md](persistence.md)'s "after a failed write" rule exists precisely for it, and the simulation tests must prove it sufficient.

Slow responses are not a separate kind; they are the delay.

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

Re-running with the same seed gives byte-identical `decision` lines — this rule itself needs a test.

**Any failure prints the whole log**, not just the decision half. The observation half exists for exactly this moment.

No JSON. When something fails, a human stares at two logs looking for the first differing line; `diff` beats everything.

## Package and tests

The `sim/` package, build tag `sim`, tests named with a `TestSim` prefix (`make sim` filters with `-run TestSim`).

`sim` depends on `gor`, `runtime`, `store`, and `clock`; none of them depend on it.

**Invariants run over a batch of seeds, not one.** One seed walks one trajectory and cannot cover several fault combinations. The seed list is hardcoded (for example 64 consecutive seeds from some base), so failures reproduce without introducing wall-clock randomness. The reproduction test, in turn, needs only one fixed seed: it tests that the skeleton itself does not draw the PRNG from multiple goroutines, not coverage.

porcupine (`github.com/anishathalye/porcupine`) is a new dependency.

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

- The fake `Transport` implementation: network partitions, message reordering, dropping.
- One `Clock` with an offset per node.
- Concurrent writes during a partition are blocked by the ETag — this is not a new invariant; it is step 4's invariant re-run under a new fault source.

Step 6c: probe failures and voting; the new invariant is "a healthy node is not killed by expired votes".

## Gap

The current fake network deterministically simulates partitions, drops, and recovery, but does not yet inject delays or message reordering. The existing DST therefore cannot cover message-timing changes.
