# Runtime

## Activation

An entity's "activation" is its in-memory instance on some node. Lifecycle:

```
absent ── call arrives ──▶ activating ──▶ active ── idle timeout ──▶ deactivating ──▶ absent
                                │                                          │
                           OnActivate                                OnDeactivate
                   reads state from the store                         persists
```

Key point: **users never explicitly create or destroy entities.** `Ref[T](rt, key)` only constructs a reference; it triggers no I/O. Only the first method call triggers activation.

## Lifecycle hooks

Two optional interfaces; the runtime calls whichever the entity implements:

```go
type Activatable interface {
    OnActivate(ctx context.Context) error
}

type Deactivatable interface {
    OnDeactivate(ctx context.Context, reason DeactivationReason) error
}

type DeactivationReason uint8

const (
    Idle DeactivationReason = iota + 1
    OwnershipLost
    RuntimeClosed
    Faulted
)
```

**Use optional interfaces, not required methods.** Most entities need neither; making them write two empty methods would be pure ceremony. Passing functions at registration time is out too — it would put "what this entity does on activation" a mile away from the entity itself.

`OnActivate` runs after state is read back from the store and before the first call enters the mailbox. If it returns an error, activation failed: the call that triggered this activation gets the error, the activation is not established, the placeholder closes with the error, and the next call starts over. There is no "half-activated" intermediate state — an entity that failed `OnActivate` yet still serves is worse than having no hook at all.

`OnDeactivate` runs right before the instance disappears, after the mailbox has been drained. It receives the `DeactivationReason` that first started the deactivation. There are only four reasons:

| Reason | What first triggers deactivation | What the app can do with it |
| --- | --- | --- |
| `Idle` | The instance idles past the timeout. | Don't treat a local reclamation as the business object going offline. |
| `OwnershipLost` | The current node no longer owns the identity, or the view has no active owner. | Release node-local leases or connections; don't announce that the business object is gone. |
| `RuntimeClosed` | The root runtime begins a graceful stop. | Do teardown before the process exits. |
| `Faulted` | A method panicked, or the entity asked to discard the current instance. | Don't treat an untrusted instance as a normal farewell; raise the alert level. |

This is the complete public set. A value may join the set only if it forces the app to make a different decision; a reason must not be added just because the implementation gained a branch. Panic and discard both mean the current instance is no longer trustworthy; migration and no-owner both mean the current node loses ownership — hence one value each.

The reason is written in the same atomic transition where `beginDeactivation(reason)` moves the activation from `active` to `deactivating`. Later calls must not overwrite it once the activation is already deactivating. If the root runtime has entered `closing`, one activation may have already started deactivating for `Idle`; its reason stays `Idle`. A deactivation reason describes why an activation first leaves; the root state machine describes whether the whole runtime admits calls, how it waits, and with which stop error it rejects calls. These are two concepts and must not share one enum.

**Returning an error changes nothing.** Deactivation cannot be rejected, and the state is in the store anyway. The error has no caller; like scheduled delivery failures, it goes to the runtime's error sink (see [timers.md](timers.md)), with no retry. The sink's source carries `Deactivation{Reason: reason}` instead of a fabricated method name.

Each normal deactivation hook gets a fresh `context.Background()`. This context has no deadline and is never canceled; it inherits nothing from any caller of the entity. A graceful stop waits for hooks that already started, so a hook must finish promptly.

**Neither `Kill()` nor this node being declared dead starts `OnDeactivate`.** An abrupt stop gives no teardown chance to hooks that have not started. Hooks that already started are not canceled, and an abrupt stop does not wait for them; handing them a canceled context would only create a third semantics of partial teardown.

This rule also covers deactivations already in flight. Idle eviction fires first and `Kill()` arrives later; an instance whose hook has not run yet must not run it either — the criterion is whether the instance was killed before its hook ran, not who initiated this deactivation. So "skip" is a fact recorded on the instance, not a parameter of some deactivation path: there can be many paths, but only one instance.

### Migration

This is a planned v0 breaking change. Existing `Deactivatable` implementations change their deactivation method to take a second `DeactivationReason` parameter. Implementations must not treat unknown reasons as normal deactivation in a default branch; the runtime only passes the four values in the table above.

## Local directory

Each node keeps a table:

```go
type activation struct {
    id       Identity
    instance any
    mailbox  *mail.Box
    lastUsed time.Time
}
```

Arriving requests are looked up: on a hit, deliver to the mailbox; on a miss, activate first.

**Activation must be idempotent and mutually exclusive** — two concurrent requests for the same key must not activate two instances. A per-key "activating" placeholder handles this: the second request sees the placeholder and waits instead of activating again.

```go
type entry struct {
    ready chan struct{}   // closed once activation completes
    act   *activation     // readable only after ready closes
    err   error
}
```

This pattern is standard Go practice (equivalent to `singleflight`), but `golang.org/x/sync/singleflight` is not used: it uses a mutex internally, and mutex blocking does not count as durably blocking in `synctest`, so the bubble could not determine quiescence. We implement it ourselves, with channels. This is a recurring trade-off: being observable to `synctest` outranks reusing an existing library.

## Idle eviction

A background loop periodically scans and deactivates activations whose `lastUsed` exceeds the threshold.

Eviction must be mutually exclusive with delivery: new requests must not be delivered to its mailbox mid-deactivation, or they land on a goroutine that is about to die. The mechanism is a state machine — once the activation enters `deactivating`, new requests take the "re-activate" path and wait for the old instance to finish persisting.

Time comes from the injected `Clock`, so this entire block can be exhaustively unit-tested under a fake clock, with no `time.Sleep`.

## Request dispatch

```
call ──▶ ring: who owns this id? ── self ──▶ runtime ──▶ local directory ──▶ mailbox
              (in gor)                   │
                                       other
                                        ▼
                             transport.Send ──▶ remote node
```

**The fork is in `gor`, not in `runtime`.** `runtime` only sees the left branch: give it an Identity, it finds or builds the activation and delivers the call into the mailbox. It does not know the right branch exists — and so single-node mode has no extra code to route around (see [cluster.md](cluster.md)).

## Reentrancy

By default, an entity does not accept a second call while processing one.

This brings the classic deadlock — A calls B, B calls A back. Orleans relaxes the restriction with `[Reentrant]` / `[AlwaysInterleave]` annotations, at the cost of the user having to reason about invariants under interleaved execution.

gor's stance: no reentrancy for now. A deadlock in gor shows up as a call timeout plus a clear error message (naming the detected call cycle). That is better than making users choose between "add an annotation to break the deadlock" and "maintain interleaving-safe invariants" — a choice they are likely to get wrong.

If practice proves it necessary, it will be added — at method granularity, not type granularity.

Call cycle detection requires carrying a set of already-occupied entities along the call chain. Go has no `AsyncLocal`; the only option is to carry it explicitly in `context.Context` — a genuine disadvantage of Go relative to .NET, see [research/go-capabilities.md](../research/go-capabilities.md) (in Chinese).

## Errors and timeouts

Every call carries a timeout (from `ctx`). The semantics of the timeout must be stated clearly: a timeout means the caller is no longer waiting, not that the entity stops executing. The method body may already have changed state.

No automatic retry is provided. The runtime does not know whether a method is idempotent; retrying on the user's behalf causes problems like duplicate charges. Retrying is the caller's decision.

## Panic handling

When a method body panics: recover, convert the panic into an error for the caller, and deactivate the activation.

An instance that panicked is not kept in service — its in-memory state may be inconsistent. The next call recovers from the store. The cost of this choice is that a panic loses unpersisted in-memory state; that is the right direction.

## Queued calls at deactivation

The mailbox is empty when idle eviction happens — if it were not, it would not be idle. But panic-triggered deactivation does not pick its moment; several not-yet-started calls may still be queued.

The rule: **queued, not-yet-executed calls all return with an error; they are not migrated to a new instance.**

Migration is rejected because it would either replay and hit the same panic again, or require deciding "is this the call that killed the instance" — and the runtime does not have that information. Returning the error lets the caller decide whether to retry, the same stance as "no automatic retry".

To the caller, this is the same kind of event as a timeout: the call did not execute, and state did not change.

## Root runtime stop state machine

The root runtime owns call admission and the stop reason. The inner execution runtime only owns activations and mailboxes; the cluster node only owns membership state; the poller and the transport are not another set of stop switches. The root state machine:

```
running ── Close ──▶ closing ── graceful stop done ──▶ stopped
   │                   │
   │                   └── Kill ──▶ killing ── abrupt stop done ──▶ stopped
   │
   ├── Kill ──▶ killing
   │
   └── node declared dead ──▶ dead ── abrupt stop done ──▶ stopped
```

There are only four transition functions: `beginClose` moves `running` to `closing`; `beginKill` moves `running` or `closing` to `killing`; `becomeDead` only moves a root runtime still in `running` to `dead`; `finishStop` moves `closing`, `killing`, or `dead` to `stopped`. There are no back edges. A repeated `Close` and a `Kill` after the runtime has already stopped do not change state; a `Kill` during `closing` is an escalation, not a no-op that waits out the pending `Close`.

### Admission is the only boundary

The atomic transition by which `beginClose`, `beginKill`, or `becomeDead` successfully leaves `running` is the linearization point of call admission. It also closes the public stop signal. A closed signal is not proof that all resources are released; it only proves that no entity call can be admitted after this point.

Every entity call first goes through the root-level `admit`. In the same serialized domain as the state transitions, it checks `running` and registers this call, and returns a release that must be called on completion. An `admit` either lands before the transition and becomes an admitted call, or lands after it and immediately gets the stop error. It must not read the state first and enter the execution runtime or start forwarding afterwards.

The following entry points all use the same `admit`; none has its own closing check:

- The public `Runtime.Invoke` admits before ownership and forwarding.
- The inbound `invoke` handler admits before handing to the local execution runtime. It must not call the inner execution runtime directly.
- Scheduled deliveries still go through the root call entry, so they are bound by the same rule.

Probes are not entity calls and do not count toward the call count; but they read the same root state and refuse to reply when it is not `running`. When an inbound request is rejected for stopping, it is not first checked against a separate `Done` check, and the result does not differ by local or forwarded origin.

`closing`, `killing`, and the `stopped` reached from either of them all return the root package's `ErrRuntimeClosed`, whose stable code is `gor.runtime_closed`. `dead` and the `stopped` reached from it return `gor.node_dead`. The cross-node reconstruction rules for these two errors are in `errors.md`; direct and forwarded calls judge by the same stable code. Internal mailbox, execution runtime, or transport errors must not supersede this root-level admission result.

### Graceful stop and abrupt stop

After entering `closing`, the poller starts no new calls, the cluster node leaves gracefully, and the mailbox closes. Admitted methods already executing may finish; calls in the mailbox that have not started are rejected per existing mailbox rules — no migration, no replay. The root runtime waits until all admitted calls have released, executing methods have finished, deactivations have completed, and the infrastructure goroutines it started have exited, then calls `finishStop`. The transport must stay open until admitted forwarded requests and inbound replies are done.

After entering `killing`, the same order applies: reject new calls first, then cancel executing methods, reject the mailbox queue, and skip deactivation hooks that have not started. It does not wait for user methods — Go cannot forcibly abort code that ignores cancellation. `stopped` is reached once the abruptly stopped runtime infrastructure has exited; it does not mean such user code has necessarily returned.

A `Kill` during `Close` must immediately escalate to the latter semantics: cancel running calls, reject the queue, and skip deactivation hooks that have not started. All calls waiting for `Close` to finish conclude under the abrupt stop as part of this escalation; the runtime must not keep waiting for user methods in the name of graceful stop. The closing interfaces of the inner execution runtime, the cluster node, and the transport must all support this escalation. A subcomponent that already received the graceful stop command must not treat a later `Kill` as a no-op.

The root runtime's waiting is expressed only with channels: admitted calls reaching zero, the execution runtime ending, the cluster node ending, the poller ending, and the transport ending each close their own completion channel; the stop coordinator only receives these channels. Short critical sections may guard a state transition or a count, but a mutex, condition variable, or `WaitGroup` must not be used to wait for another call to complete.

### Death declared by the cluster

The cluster node must report why it ended; it must not just close a reasonless `Done` channel: an active `Close` also writes the node's membership row as `dead`, which is not the same as the root runtime being declared dead by others. When the root runtime, still in `running`, receives an external declaration of death, it calls `becomeDead`: first closes admission and the public stop signal, then concludes under abrupt stop. A root runtime already in `closing` or `killing` keeps its state.

Thus the inner execution runtime can still drain while `closing`, but it is no longer an externally reachable "stop signal closed, yet still admitting calls" window. That window is now a state with a name, admission rules, and completion conditions — not a gap left behind by goroutine execution order.

### Gap

The root runtime's stop state machine is implemented: the four transition functions `beginClose`, `beginKill`, `becomeDead`, `finishStop`, with the atomic `admit`/release as the only admission gate; the public `Runtime.Invoke`, the inbound `invoke` handler, and scheduled deliveries share the same entry and admit before ownership and forwarding. `closing`/`killing` and the `stopped` reached from them return `gor.runtime_closed`; `dead` and the `stopped` reached from it return `gor.node_dead`.

Stop coordination is implemented as pure channel waiting: the execution runtime exposes `BeginClose`/`BeginKill` plus a `Done()` channel, the cluster node exposes a `DeclaredDead()` channel, and the root coordinator in `closeGracefully`/`closeImmediately` receives only `clusterDone`, `engine.Done()`, `drained`, and `transportDone`. The inner execution runtime supports the `closing → killing` escalation (`BeginKill` from `closing` is not a no-op: it closes the `killing` channel, marks deactivation hooks that have not started to be skipped, and cancels execution). The cluster node explicitly reports "declared dead externally" via `DeclaredDead()` rather than "exited on its own", and the root layer no longer infers the reason from whether it initiated the stop itself. A declared-dead node no longer publishes the final empty view, so graceful migration and abrupt stop do not race to start.

Transport teardown is implemented: in a graceful stop, `closeTransport` runs after `<-engine.Done()` and `waitDrained()`, so the transport closes only after admitted local calls, inbound replies, and forwarded requests are done (forwarded requests hold a root inflight until the transport round trip returns). The root layer tracks forwarded requests' transport round trips indirectly through the root inflight (the admission count); forwarded requests have no separate round-trip completion signal. The current two-node fake network already verifies "the transport does not close before in-flight forwarded requests complete their transport round trip"; connection-level interruption semantics for the real transport are the transport implementation's responsibility.

Deactivation reasons are implemented: `activation` saves the reason in the same atomic transition of `beginDeactivation(reason)`; `waitForDeactivation` and `skipOnDeactivate` read it in the same critical section and hand it to the hook. The reason is written only in that transition; later events (including the root runtime having entered `closing`) do not overwrite it. The four entry points map one-to-one onto the table above: idle eviction passes `Idle`, `Deactivate` (view eviction or no active owner) passes `OwnershipLost`, `beginStopDeactivationsLocked` passes `RuntimeClosed`, and `stopActivation` for panic and discard passes `Faulted`. Each hook gets a fresh `context.Background()` (no deadline, never canceled, inheriting no caller context); `Kill()` and being declared dead still skip hooks that have not started, and hooks that already started are neither canceled nor waited for. Hook errors are reported through the structured sink, with source `Deactivation{Reason: reason}`; see [timers.md](timers.md).

The only reason `Kill()` exists is simulation tests — a real process crash does not politely call a function first. It is not a shutdown API for users; users shut down with `Close()`.
