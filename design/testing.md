# Testing

> This is the most important design document in the project. The testing strategy here is an **architectural constraint**, not the internal business of the testing team.

## Why it is an architectural constraint

Orleans' test code measures 177k lines across 1168 files, of which **258 files depend on `TestCluster` (spinning up a real in-process cluster) and 136 files contain `Task.Delay` or `Thread.Sleep`**.

In other words: correctness hides in timing. A passing test means "lucky this time", not "the invariants hold". Such a suite can serve neither as an oracle for the port nor as a guardrail for refactoring.

`gor` does not repeat this path. The constraints apply from day one, because they **cannot be retrofitted**:

1. All I/O is behind interfaces.
2. All time comes from an injected `Clock`; `time.Now()` in production code is a bug.
3. Components are explicit state machines — state transitions are enumerable functions, not implicit flows scattered across goroutines.
4. Cross-call waiting uses channels, not mutexes (reason below).

Rule 4 is the easiest to violate and the most insidious in consequence, so [scheduling.md](scheduling.md) covers it separately.

## Two tracks

**Unit tests** (`make test`) — verify a single package: single-node runtime, mailbox, store, hash ring, generator. Requirements: each test under 50 ms, no network, no subprocesses.

**Simulation tests** (`make sim`) — verify distributed invariants. Fake network + fake clock + fault injection + fixed seeds. Slow, on a separate target, out of the default `test`.

Naming and placement must make the verification subject readable: `TestMailbox_SerializesCalls` is a unit test, `TestSim_PartitionDoesNotLoseWrites` is a simulation test.

## What synctest can and cannot do

`testing/synctest` is GA in **Go 1.25** (the only reason `go.mod` requires 1.25). It provides `synctest.Test` to set up a goroutine bubble; time inside the bubble is fake, and `synctest.Wait()` waits until every goroutine in the bubble is "durably blocked".

This removes the biggest source of flakiness in tests, "waiting for an async process to finish": no more `time.Sleep(100 * time.Millisecond)` followed by a prayer.

**Its boundaries must be remembered**:

- Real network I/O and real syscalls break the bubble's quiescence judgment. So the fake network is a necessity, not a bonus.
- **Blocking on a mutex is not durably blocked.** For code that waits with a mutex, `synctest.Wait()` cannot judge quiescence; the test hangs or misjudges. This is where rule 4 comes from.
- It is a unit-testing tool, not a DST framework — it does not control goroutine scheduling order.

## How simulation tests are built

There is no usable off-the-shelf DST framework in Go; this must be stated clearly so later comers do not go looking:

- **gosim** (`jellevandenhooff/gosim`) has the right shape (multi-machine, deterministic goroutine scheduling), but only 80 stars and **abandoned after 2024-12**. Cannot be relied on.
- **Antithesis** works; quoted 168k USD/year in 2025-09. Not in this project's cost structure.
- **porcupine** (`anishathalye/porcupine`, 1230 stars, active) is a linearizability checker: it verifies whether a history is linearizable, but does not control scheduling. Useful, but only one piece of the puzzle.

Resonate's public conclusion is the same: goroutine scheduling cannot be controlled in Go, so DST must invasively constrain the whole codebase.

So the `sim` package builds its own. Below are the pieces it needs; how they are built in [simulation.md](simulation.md):

**Fake network** — implements the `Transport` interface. It can decide message delay, reordering, dropping, and partitioning by seed. A partition is a set of "which node pairs cannot talk"; it can change at any time.

**Fake clock** — uses `synctest`'s time inside the bubble; cross-node time offsets come from a per-node offset in the `Clock` implementation, for testing clock skew.

The fake clock's ticker **must drop ticks like a real `time.Ticker`**: when the receiver cannot keep up, ticks are dropped non-blockingly instead of waiting for it to read. A blocking send lets a stuck consumer drag down the hand that advances time — and the whole point of fault injection is to stick the consumer. This is the same line as [timers.md](timers.md)'s "missed windows are not made up": however many due times piled up, waking fires once.

Dropping ticks imposes a hard requirement: **clock subscription must happen in the constructor, not inside a goroutine.** If `NewTicker` runs inside `go c.run()`, there is a window between the constructor returning and the subscription taking effect; advance time inside that window and that tick is dropped outright, while the next tick must wait a full period. The symptom is an intermittently failing test — the component looks alive but does not move.

"Constructor returns ⇒ its clock subscription is already in effect" must be an invariant. Then callers do not need to know the window exists, and tests do not need a `synctest.Wait()` first to work around it.

**Fake store** — an in-memory `Store` implementation that can inject write failures, slow responses, and "the write succeeded but the response was lost", the easiest case to get wrong.

**Fault injection** — node crash (drop all in-memory state, keep the store), node pause (simulating a long GC), network partition, partition recovery.

**Invariant assertions** — checked after every step. The core ones:

- State is never silently overwritten (a double activation hitting the ETag must report a conflict).
- The call history of the same entity is linearizable (handed to porcupine).
- Scheduled tasks are not delivered twice.
- Membership views eventually converge.

**Seed reproduction** — on failure, print the seed; re-running with the same seed must produce a byte-identical event sequence. This rule itself needs a test.

## Forbidden

- No real external dependencies: real network, real processes, real databases, real filesystems (at the unit-test level).

  **The only exception is the embedded storage backend's own tests.** A SQLite backend cannot be verified to really persist data without touching disk. These tests use `t.TempDir()` and test only that one backend package. Every layer above — `runtime`, `gor`, `sim` — uses the in-memory `Store`. The exception stops here; do not let it spread.
- No real time: no `time.Sleep` for synchronization, no `for now < deadline` polling for assertions.
- No flakiness: no dependence on test execution order, no timestamps as seeds, no `t.Skip` to cover up intermittent failures. Treat flakiness as a bug to fix, not noise to tolerate.
- No old and new tests side by side: once the migration is done, delete the old ones.

## Relationship with the ROADMAP

The DST skeleton is [step 4](../ROADMAP.md#4-deterministic-simulation-test-skeleton) of the ROADMAP, **before the cluster ([step 6](../ROADMAP.md#6-multiple-nodes))**.

The order cannot be swapped. Adding any of the four constraints above after the cluster is written means rewriting the cluster. This is the project's only "the order is non-negotiable" spot.
