# Scheduled tasks

One table plus one poller. The poller finds rows that have come due and delivers an ordinary call to the target entity.

The entity sees no difference: a due call goes through the same mailbox as any other call, equally serialized.

## The method name, not a function value

```go
type account struct {
    balance  gor.State[int64]
    schedule gor.Schedule
}

func newAccount(b *gor.Binder) *account {
    return &account{
        balance:  gor.NewState[int64](b, "balance"),
        schedule: gor.NewSchedule(b),
    }
}

func (a *account) Open(ctx context.Context) error {
    return a.schedule.Set(ctx, "monthly-interest", gor.Every(30*24*time.Hour), "ApplyInterest")
}

func (a *account) ApplyInterest(ctx context.Context) error { ... }
```

**Only names can be stored in the table.** After a process crash, nobody can deserialize a closure back; so the scheduled task records the method name, and the poller sends an ordinary call by that name.

The called method only takes `ctx` and only returns `error`, and it must be in the entity's interface — the dispatch table is produced by the generator from the interface, and a method not in it cannot be found at delivery time. A name that does not match makes delivery return an error — no registration-time check for this; a mistake is visible on the spot.

"One unified entry point" is rejected. Orleans has entities implement `ReceiveReminder(name)` and switch on the name themselves — that is bringing back the hand-written dispatcher deleted at [step 3](../ROADMAP.md#3-类型化代理代码生成), and in user code of all places. gor's selling point is compile-time typing; it must not open a string-dispatch loophole here.

The method name is still a string; that is a gap. Closing it requires the generator to emit typed method handles — the generator's job, not this step.

## The handle comes from the Binder

Like `State`: `gor.NewSchedule(b)` binds identity and storage at entity construction, and the methods use it directly afterwards.

Not fished out of `ctx`. Hiding runtime capabilities in `context.Value` makes "what this code needs" invisible and forces tests to build the right ctx before they can run. Constructor parameters are explicit; ctx is not.

## When it comes due

Two constructors, enough:

```go
gor.After(d)   // one-shot, fires once after d
gor.Every(d)   // periodic, fires every d
```

`Set` overwrites by name: only one task per name on the same entity; setting again reschedules rather than adding another. `Cancel(ctx, name)` deletes it.

**Missed windows are not made up.** If the process is down for three periods, it fires once on return and then tracks to the next future time. Making up three firings is a trap — what users want is almost never "run everything that piled up", and how much piles up depends on the downtime, making the behavior unpredictable. If catch-up is truly wanted, users compute it in the method from the last execution time.

**Precision is the polling interval.** Persisted scheduled tasks should never promise milliseconds.

## The table

```
schedule(entity_type, entity_key, name, method, due_at, interval, etag)
```

A zero `interval` means one-shot.

The primary key is (entity_type, entity_key, name). `name` identifies the task; `method` is the method to call when due — both are needed, because the same method can back several tasks with different periods.

## The table's interface

Four operations, aligned with what the poller and the user each need to do:

- **List due** — rows with `due_at <= now`; `now` comes in as a parameter.
- **Claim one row** — CAS with the row's etag, pushing `due_at` to the given next time; a zero time deletes the row. Exactly one claiming node wins.
- **Write one row** — unconditional overwrite; the user's `Set` goes here.
- **Delete one row** — unconditional; the user's `Cancel` goes here.

**The etag exists only for claiming.** The user's `Set` / `Cancel` carries no etag: the user does not have one anyway, and an explicit reschedule or cancel is his to win. The claim that got overwritten simply delivered one fewer time; at-most-once still holds.

**The next due time is computed by the poller, not the table.** "No catch-up for missed" is policy; the table is only responsible for getting the CAS right. One-shot tasks use the zero time for "no next" — the same convention as a zero `interval`.

No row-count limit on "list due". Add it when it is actually needed; adding it now decides for a scale that does not exist yet.

## Claim first, then deliver

The poller scans rows with `due_at <= now` and, for each row:

1. **Claim** — CAS to push `due_at` to the next period (delete the row for one-shot tasks).
2. Deliver the call only after winning the claim.

**Push to the first time still in the future**, not `due_at + interval`. After three periods of downtime, adding one interval still lands in the past; the next scan hits the same row again, and "no catch-up" becomes catch-up.

The reverse order causes repeated firing on a crash. Crash after the claim but before delivery misses one firing — a deliberate trade-off:

**gor promises at-most-once delivery, not exactly-once execution.**

Delivery failures are not retried. Only the user knows whether retrying is safe; the runtime does not decide for him — the same stance as on `State.Set()` conflicts.

**But no retry does not mean silence.** A scheduled delivery has no caller waiting; the error returned by the method is sent by the runtime to the configured error sink, and dropped only when no sink is configured. The runtime does not retry for the user; whether to alert remains the user's decision.

So the configuration needs an error sink:

```go
type BackgroundError struct {
    Identity Identity
    Err      error
    Source   ErrorSource
}

type ErrorSource interface {
    errorSource()
}

type ScheduledInvocation struct {
    Method string
}

func (ScheduledInvocation) errorSource() {}

type Deactivation struct {
    Reason DeactivationReason
}

func (Deactivation) errorSource() {}

func OnError(func(BackgroundError)) Option
```

`ErrorSource`'s unexported method seals the set inside the `gor` package; code outside the package cannot implement new sources. Every event is constructed by `gor`, and there are only two sources: claimed scheduled deliveries use `ScheduledInvocation{Method: ...}`, deactivation hook failures use `Deactivation{Reason: ...}`. Callers branch on the concrete type of `Source`, not on comparing writable strings; so a scheduled method exactly named `"OnDeactivate"` cannot be confused with a deactivation source.

**One sink, not one per scheduled task, not one per event kind.** It only reports the two kinds of application callback failures with no caller to receive them: claimed scheduled deliveries and normal deactivation hooks. Direct calls return errors to the caller as usual. Polling scans, claim failures, and losing the CAS do not enter the sink — they are operational states of the scheduler and storage, not failures of a known application action.

`Err` is exactly the error the callback got. It follows [errors.md](errors.md): across nodes, only the stable `Code` is usable with `errors.Is`; the event does not add its own `Code` field, nor does it restore error types, fields, or wrapping.

It does no retry, no backoff, no alerting policy — those are the user's business; the runtime only delivers "this failed" into the user's hands. The event carries no schedule name, due time, interval, ETag, or attempt count: after claiming these fields may already be stale, the ETag is not an application decision, and the runtime has no retry model. `timer.Invoker` keeps receiving only identity and method.

When unconfigured, these two kinds of errors are dropped. They are the only application callback errors the runtime drops on the user's behalf, and must be written in the docs, not hidden in the implementation.

**A delivery canceled mid-shutdown is not a failure.** At runtime shutdown, in-flight scheduled calls come back with a cancellation error — the method did not fail; the runtime stopped running. Sending it to `OnError` would report a false alarm to the user on every clean shutdown, and users would have to filter cancellation errors out in their own callbacks. So when the poller's context is already canceled, this error does not go out.

This is not defensive special-casing; it is behavior a test must watch: cancellations during shutdown do not enter `OnError`; other callback errors matching the sink boundary above must enter.

**Claiming must be a CAS, not "read then update".** The pollers of two nodes can scan the same row at the same time; CAS is the only thing that makes one of them lose. This must be right now, not deferred until step 6 — by then, every earlier test would need rewriting.

### Migration

This is a planned v0 breaking change. Existing three-parameter error handlers change to receive one `BackgroundError`. Scheduled deliveries read `ScheduledInvocation.Method`; deactivation hook failures read `Deactivation.Reason`. Existing code branching on `method == "OnDeactivate"` must be deleted.

### Gap

The error sink is implemented: `OnError` receives a `BackgroundError` (entity, original error, source), and the source is a sealed set — `ScheduledInvocation` carries the method name, `Deactivation` carries the deactivation reason, and the unexported method prevents additions outside the package. `Err` is the error the callback got; the event has no separate `Code` field and does not restore error types, fields, or wrapping. The cross-node stable-code contract is governed by [errors.md](errors.md); the event itself never crosses nodes. The poller does not report errors from scans, claims, or losing the CAS; when the poller's context is canceled, that delivery's cancellation error is not reported (the criterion is context state, not error shape). This section's migration is complete: the old three-parameter handlers are deleted, and production code no longer classifies errors by `method == "OnDeactivate"` (test fixtures' dispatchers dispatching deliveries by method name is delivery scheduling, not error classification).

## Don't claim rows that are not yours

In a cluster, every node's poller scans the whole table, but a row's target entity belongs to exactly one node. Before claiming a row, ask whether it is yours; if not, skip.

Not asking loses deliveries, not duplicates them: when a non-owner wins the claim, `due_at` has already been pushed (the row is deleted for one-shot tasks), and then the call is rejected by routing — that due time is gone forever, and the owner's poller will not see it next round.

So the poller must ask one more thing: not just "call this method", but "do I own this identity". In a single node the answer is always yes — that is not a fake implementation; a single node indeed owns everything.

With an inconsistent view, two nodes may both believe they are the owner; CAS makes one lose, and at-most-once holds. If both believe it is not theirs, this due time is deferred until the view converges; at-most-once still holds.

## Coming due activates the entity

If the target entity is not in memory, delivery activates it. This is the point of persisted scheduled tasks: without it, an evicted entity would never see its next wake-up.

## The poller

One goroutine, driven by the injected `Clock`. `Runtime` must make it exit whether `Close()` or `Kill()` is taken — in `sim`, a goroutine that does not exit makes the whole bubble judged deadlocked.

The poller itself has only "running" and "stopped"; no state enum is invented for it: a two-state state machine is ceremony, not design. It does not distinguish draining from non-draining stops either — it carries no user state, and a delivery canceled halfway still falls within the at-most-once promise.

It gets its own package. `runtime` cannot host it — the poller reads tables, and `runtime` does not import `store`. `gor` should not host it either — that layer only assembles configuration; it does not hold algorithms. So the poller, like `mail`, is a small package: it takes a table interface, a `Clock`, and an interface for initiating calls; `gor` wires the three together.

## A new I/O interface

The scheduled task table does not go through `store.Store`. That interface is "read and write one state per Identity"; scanning due rows, CAS advancement, and row deletion do not fit.

A new interface, shaped by what the poller actually does: list due, claim one row, write one row, delete one row.

It is a new fault source on the step-4 skeleton: the fake implementation injects by seed — scan failures, claim failures, and claim succeeded but the reply was lost. The third is the most important: the claim landed but the poller does not know — exactly where duplicate delivery is most likely.

## Scheduled task writes share no transaction with state writes

`schedule.Set()` writes to a different table, not the same transaction as `State.Set()`. So there is a window where the task is set but the state is not persisted.

The two tables are not bound into one transaction so that backends are not tied together — coordination tables must be able to live on Postgres in the future, while the state table may live elsewhere. The cost is written here; users decide whether it matters.

## Invariants

The step-4 skeleton must hold this one:

- **One delivery per due time.** For the same (entity, name) and the same `due_at`, history contains at most one delivery.

Crashes, claim failures, two pollers scanning at the same time — none of these may break it.

## Gap

The method name is a string. The generator does not yet produce typed method handles.
