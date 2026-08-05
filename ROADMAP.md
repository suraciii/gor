# Roadmap

**Current status:** steps 1 through 5.5, step 6a (membership table and ring), step 6t (transport), step 6b (cross-node forwarding), and step 6c (probing and death voting) are implemented.

Slicing principle: every step runs, is accepted, and delivers value on its own. Dependencies of the form "step 1 cannot be verified until step 6" are not allowed.

## Steps

### 1. Single-process runtime

A per-key mailbox (one goroutine + one channel), an activation cache, and idle eviction. No network, no persistence, no cluster.

Estimated around 1500 lines. Design: [design/runtime.md](design/runtime.md), [design/scheduling.md](design/scheduling.md).

The injectable `Clock` belongs to this step, even though the DST skeleton is step 4. Idle eviction must be accepted without touching the wall clock, and that already requires time to be injected. Step 4 completes the fake network and fault injection; it does not start introducing the fake clock here.

Method dispatch is handwritten in this step. Besides the factory, `Register` takes a dispatch function that turns method names and arguments back into direct calls on the implementation type — exactly what the generator in [design/codegen.md](design/codegen.md) will produce. Step 3 replaces the handwritten version with the generated one; the seam does not change. This step does not use reflection for dispatch: the reflection version would be deleted wholesale in step 3, while the handwritten version is just a human standing in for the generator.

**Acceptance:** concurrent calls to the same key are strictly serialized; different keys run concurrently; after the idle timeout the object is evicted and the next call reactivates it; all covered by unit tests that do not depend on the wall clock.

### 2. Persistent state

State read/write for objects, plus one table with CAS. The storage backend is embedded (one of SQLite / bbolt / pebble; the choice is in [design/persistence.md](design/persistence.md)).

The registration factory signature changes from `func() T` to `func(*gor.Binder) T` in this step. Step 1 has no storage, so no placeholder `Binder` is added early. This is a planned breaking change, not an oversight.

**Acceptance:** state is restored after a process restart; concurrent write conflicts are rejected by CAS instead of silently overwritten.

### 3. Typed proxy code generation

Generate proxy implementations from user-written Go interfaces, replacing `any` in, `any` out. Design and precedent: [design/codegen.md](design/codegen.md).

The `gor.Register` signature changes from `(rt, factory, dispatch)` to `(rt, factory)` in this step — the generator takes over the handwritten dispatch function from step 1. `gor.Ref` is added at the same time. Like the step 2 factory signature, this is a planned breaking change.

**Acceptance:** calling a remote object's method from user code uses exactly the local interface's signature; a wrong argument type is a compile error, not a runtime panic.

### 4. Deterministic simulation test skeleton

Seed-driven fault injection, `testing/synctest` fake clocks, and porcupine for linearizability checking. Design: [design/simulation.md](design/simulation.md).

This step must come before the cluster. DST cannot be retrofitted — it requires all I/O behind interfaces, all components as explicit state machines, and no direct wall-clock reads anywhere. Adding it after step 6 would mean rewriting step 6. Reasons: [design/testing.md](design/testing.md).

The fake network is not in this step. There are no cross-node calls yet, so there is nothing to inject; step 6 hangs a new fault source on the skeleton built here. The skeleton itself must be completed here: seeds, fault injection on the fake store, node crash and restart, event log, invariant assertions.

Node crashes require `runtime` to gain a stop path that does not drain. This is a planned addition, not a breaking change.

**Acceptance:** a fixed seed reproduces a sequence of injected store faults and node crashes, and a rerun yields a byte-identical decision sequence; invariants hold under those faults; a double activation created by two nodes sharing one store is blocked by the ETag on write conflict instead of being silently overwritten.

What is reproduced is the injected decisions, not the whole execution. A fault deactivates the activation, and from then on even the entity's value depends on scheduling — see [design/simulation.md](design/simulation.md).

### 5. Scheduled tasks

One table plus one poller. Design: [design/timers.md](design/timers.md).

Deliberately not repeating Orleans Reminders v1's design — the in-memory cache plus ring-partitioning scheme is what Orleans itself replaced in v2 (`Orleans.DurableJobs`). Start directly with a table plus a poller.

The scheduled-task table does not go through `store.Store`; it is a new interface. It is also a new fault source on the step-4 skeleton — it must be hooked up in this step, not deferred to step 6.

Preemption via CAS must be done right now. In step 6, two nodes' pollers can scan the same row at the same time; fixing it then would mean rewriting all the earlier tests.

**Acceptance:** a task that has come due still fires after a process crash; the same due time is delivered at most once, and this must hold as a simulation-test invariant under injected preemption faults and node crashes.

### 5.5 API fixes from the example

Implemented. The example's factory now takes only `*gor.Binder`; the load generator no longer narrates eviction — it first asserts the local activation directory is empty, then reads state back, and that assertion fails when eviction is off.

The friction from writing the first real example was minor, but all of it sat on the main path:

- `gor.Now(b)` — the Binder already holds the injected `Clock`; without it, users would write `time.Now()`.
- `gor.Ref[T](b, key)` — an entity calling another entity should not require the factory to capture a runtime object.
- `OnError` — scheduled delivery failures used to be dropped silently; they are now visible to users through the unified error sink.
- `OnActivate` / `OnDeactivate` — the lifecycle hooks used to be missing; they are now implemented as optional interfaces, so the example can be notified on activation and eviction.

The first two are in [design/persistence.md](design/persistence.md), the third in [design/timers.md](design/timers.md), the fourth in [design/runtime.md](design/runtime.md).

Placed before step 6 because it changes the public API. API changes get more expensive the later they come, and the example app is waiting on the new signatures to get the README right.

### 6. Multiple nodes

A consistent-hash ring, shared-table membership, and death voting. Design: [design/cluster.md](design/cluster.md).

This step must state plainly, in both docs and API: the directory is eventually consistent, a double-activation window exists while the cluster is unstable, and state must therefore carry an ETag. Orleans itself has this semantics (see [research/orleans-internals.md](research/orleans-internals.md) (in Chinese)); do not pretend to do better.

Too big; sliced into four segments. The split points are chosen on "does it need the network" — the dividing line is transport, then probing.

6t depends on none of the earlier segments and can run in parallel with 6a: it only deals with operating-system sockets and knows nothing of entities, identities, or the membership table.

#### 6a. Membership table and ring

A membership table, a node state machine (joining / active / dead), view polling, a hash ring, and local routing decisions. No transport: if the computed target is not this node, return an error carrying the owner's address.

A new table and a new fault source, shaped like step 5's scheduled-task table — deliberately so; step 5 just blazed this trail.

6a's membership-table-and-ring stage only defines member states and the view; the evidence for declaring death is completed by 6c's probe voting.

Implemented. The two boundaries of declaring death are written into [design/cluster.md](design/cluster.md): a failed table read does not make anyone dead, and a death declaration enters the view only after its CAS lands. A node declared dead drops all its activations and closes `Done()`.

**Acceptance:** after a node joins, other nodes can see it; after a crash it is declared dead and removed from the ring; after the view converges, the same key lands on exactly one node; when the view changes, activations the node no longer owns are dropped. All of this must live in the simulation tests and hold under membership-table fault injection.

#### 6t. Transport

A thin, self-written transport: long-lived connections, multiplexing, frames, lazy dialing. Design: [design/transport.md](design/transport.md).

It knows nothing of entities, identities, or the membership table — it moves bytes. So it does not depend on 6a and can proceed in parallel. Implemented.

**Acceptance:** out-of-order responses match their requests; a response that arrives after a timeout is dropped, not handed to the next request; when a connection breaks, every in-flight request returns with an error and no goroutine leaks; an oversized frame does not make the peer allocate memory based on the frame header.

#### 6b. Forwarding

Wire 6a's routing decisions to 6t's transport, plus a fake network (delay, packet loss, partition). Envelope and forwarding semantics: the "Forwarding" section of [design/cluster.md](design/cluster.md); how the server side recovers types from bytes: [design/codegen.md](design/codegen.md).

Implemented. Calls to entities not on this node are forwarded through the transport to the node that currently owns them, sharing the same call path as local calls; the fake network currently simulates partitions, drops, and recovery deterministically. Delay and reorder injection are not implemented yet; probing and death voting are 6c.

This step already changed the generated artifacts. `Invoke`'s argument went from `[]any` to `any`; each method has one request struct, and each type has one constructor like `newAccountCall`. Like step 3 taking over `dispatch`, this is a planned breaking change.

**Acceptance:** DST scenarios cover network partitions and partition recovery; concurrent writes from the double activation during a partition are blocked by the ETag instead of silently overwritten; the view reconverges after the partition heals; forwarded calls and local calls go through the same `Invoke`, not a second execution path.

#### 6c. Probing and death voting

Direct probing of ring neighbors, death votes with expiry, and a node's self health check.

Replaces 6a's coarse death decision of only looking at `iam_alive_at`. The `suspect_votes` column in the table starts being written only in this step. The design is complete, in [design/cluster.md](design/cluster.md): a single-point probe ring, the `Prober` interface, the parameter table, CAS merging and expiry of votes, the `min(2, n-1)` threshold for declaring death, and giving up the voting right when the self-check fails. The envelope's `kind` field is also introduced in this step.

Implemented. Nodes judge neighbor health by directly probing the ring, using neighbor death votes with expiry and landing death declarations in the membership table via CAS; a node whose self-check fails gives up its voting right, and stale heartbeats, failed table reads, and missing heartbeats are no longer evidence of death.

The boundary: the two sides of a partition can vote each other dead, even to the point where every node stops serving; recovery requires rejoining the membership table with a new generation — old rows do not resurrect themselves.

**Acceptance:** a node that is network-isolated but process-healthy is voted dead; old votes left behind by flapping do not wrongly kill a healthy node once they expire.

## Required before release

Not part of any step above, but must be completed before a public release:

- English documentation. Done last — the docs are still changing; translating early means translating twice.
- ~~Public API doc comments. To be completed after step 6c, once the public API is finalized as a release candidate; must meet [design/api-documentation.md](design/api-documentation.md) before `v0.1.0`.~~ **Done.**
- ~~Error and cancellation contract. Stable error codes and the cross-node cancellation boundary must be implemented before `v0.1.0`.~~ **Done.** Spec: [docs/errors.md](docs/errors.md) and [design/errors.md](design/errors.md). The stable code is the only cross-node identity of an error; the cancellation boundary is implemented per spec. The spec previously had one self-contradiction (merged errors matched locally but not across nodes); it was ruled that "the error code is the unique reachable value in the error tree", and the implementation was brought in line.
- ~~Root runtime shutdown contract. The spec is complete, see [design/runtime.md](design/runtime.md), [design/cluster.md](design/cluster.md), and [docs/programming-model.md](docs/programming-model.md); implementation had not started. Before `v0.1.0`, new calls must stop being admitted during the shutdown window.~~ **Done.** The root runtime's stop state machine and single admission gate are implemented: four transition functions, atomic `admit`/release; the public `Invoke` / inbound handler / scheduled delivery share one gate that sits before the ownership decision and forwarding; `closing→killing` is an escalation, not a no-op; cluster nodes explicitly report their end reason via `DeclaredDead()`; stop coordination uses receive channels only. Transport teardown comes after admitted forwarded requests and inbound replies. Also fixed a real bug where a declared-dead node sent an empty view and triggered a graceful deactivation.
- ~~Deactivation reasons and the background error sink for lifecycle hooks. The spec is complete, see [design/runtime.md](design/runtime.md), [design/timers.md](design/timers.md), and [docs/programming-model.md](docs/programming-model.md); the hooks themselves were implemented, but the deactivation reasons (`DeactivationReason`) and the structured background error sink (`BackgroundError`) were not — both are public API breaking changes. These two must be delivered before `v0.1.0`.~~ **Done.** `OnDeactivate` receives the deactivation reason (idle, ownership lost, normal shutdown, instance untrusted); the reason is fixed at the first transition out of the active state, and the hook gets a work context with no deadline that is never canceled; the background error sink now emits events whose sources are a closed set — scheduled delivery carries the method name, deactivation hook failure carries the deactivation reason, nothing outside the package can add sources, and sources are no longer guessed from method names. Poller scan and preemption failures and deliveries canceled mid-shutdown do not enter the sink. Two public API migrations ship with this item (`OnDeactivate` gains a parameter, `OnError` takes an event).
- ~~A real example application, rerun with the new signatures after step 5.5~~ **Done.** See [examples/shadow/](examples/shadow/); design: [docs/example.md](docs/example.md). Its output is [FINDINGS.md](FINDINGS.md) — nine API frictions; the first six went into step 5.5, the README's non-goals, or doc additions; the last three record frictions that still exist.
- ~~Performance baseline numbers and the cross-node forwarding baseline~~ **Done.** Numbers: [benchmarks.md](benchmarks.md); run with `make bench`. What is measured, what is not, and how conditions are written: [design/benchmarks.md](design/benchmarks.md).
- ~~Observability~~ **Done.** See [design/observability.md](design/observability.md): the runtime provides a snapshot of this node's activations and a completion event per call; no aggregation, export, or alerting.
- Versioning and release. What users can rely on: [docs/compatibility.md](docs/compatibility.md); version numbers, the v1 bar, and the concrete release checklist: [design/release.md](design/release.md).

## Risks

Only step 6 carries real distributed risk, and it contains no consensus algorithm — Orleans' membership outsources linearizability to a CAS-capable table, and so does `gor`. Steps 1 through 5 contain no distributed invariants; their risk is ordinary engineering risk.

- **DST coverage risk.** The current fake network deterministically simulates partitions, drops, and recovery, but does not support delay or message-reorder injection; existing DST scenarios can therefore cover coarse-grained network faults but not changes in message timing.

The real risks are not technical:

- **Ecosystem risk.** Orbit (EA's JVM virtual-actor implementation, inspired by Orleans) reached 1724 stars and was rewritten in Kotlin once, then was completely abandoned after 2021-06. Projects in this spot have died.
- **Single-person maintenance.** goakt's situation shows how much this hurts credibility.
