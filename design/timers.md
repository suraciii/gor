# Reminders

One table plus one poller. The poller finds rows that have come due and delivers an ordinary call to the target Grain.

The Grain sees no difference: a due call goes through the same mailbox as any other call, equally serialized.

## The method name, not a function value

```go
type account struct {
    balance  gor.State[int64]
    reminder gor.Reminder[Account]
}

func newAccount(b *gor.Binder) *account {
    return &account{
        balance:  gor.NewState[int64](b, "balance"),
        reminder: gor.NewReminder[Account](b),
    }
}

func (a *account) Open(ctx context.Context) error {
    return a.reminder.Set(ctx, "monthly-interest", gor.Every(30*24*time.Hour), gor.Handle(Account.ApplyInterest))
}

func (a *account) ApplyInterest(ctx context.Context, tick gor.TickStatus) error { ... }
```

**Only names can be stored in the table.** After a process crash, nobody can deserialize a closure back; so the Reminder records the method name, and the poller sends an ordinary call by that name. What lives in the table is a string; nothing below changes that.

**But authoring is typed, not a string.** The last argument to `Set` is a method handle built from a Go method expression on the Grain's interface:

```go
type Reminder[T any] struct { /* bound to one GrainId */ }

func NewReminder[T any](b *Binder) Reminder[T]

type ReminderTime struct { /* first delay and period */ }

func After(delay time.Duration) ReminderTime
func Every(period time.Duration) ReminderTime

type TickStatus struct {
    FirstTickTime   time.Time
    Period          time.Duration
    CurrentTickTime time.Time
}

type MethodHandle[T any] struct { /* unexported: the method name */ }

func Handle[T any](m func(T, context.Context, TickStatus) error) MethodHandle[T]

func (s Reminder[T]) Set(ctx context.Context, name string, when ReminderTime, m MethodHandle[T]) error
```

The public Reminder API is `Reminder[T]`, `ReminderTime`, `NewReminder`,
`ReminderStore`, `ReminderInvocation`, and `TickStatus`. `Handle` accepts
`func(T, context.Context, TickStatus) error`. `After` creates a one-shot
Reminder. `Every` creates a periodic Reminder.

`Account.ApplyInterest` is a Go method expression. The compiler checks that
`ApplyInterest` is a method of `Account` and that its signature is
`func(Account, context.Context, TickStatus) error`. A typo, rename, or
signature drift is a compile error. The type parameter ties the handle to the
Grain's interface.

The name the table stores is read off the method expression once, when `Handle` is called, with `reflect` and `runtime.FuncForPC`. The format of that name is not a Go-documented contract — it is empirically stable across the Go versions in use, but Go is free to change it. The implementation must therefore carry a unit test that locks the map from an interface method expression to its trailing-segment method name, so a Go upgrade that changes the encoding breaks the test instead of silently mis-naming Reminders. That runs at scheduling setup, never on the delivery path; what the poller reads at delivery is the same `method` column as before. `Handle` takes a method expression on the Grain interface — a hand-written closure of the same function type also compiles, but the name read off it is not a real method name and delivery fails with "unknown method". The contract is stated, not guarded: the type signature admits only `func(T, context.Context, TickStatus) error`, and the rest is the caller using the documented form.

The called method must be in the Grain's interface. The generated dispatch
table must accept the `TickStatus` value at delivery time.

The generator must also expose a typed `newReminderCall(method, TickStatus)`
factory for each Grain interface. The factory creates the normal typed request
and reply values for the Reminder method. The timer passes those values to
`Runtime.Invoke`, so local and forwarded Calls use the same path. The timer
must not use reflection. The public API must not expose a string dispatch path;
the stored method name is only an internal identifier used by generated code.

**Why a method expression, not a generated handle value.** Reminders are set from inside Grain methods, which live in the Grain package. The Grain package cannot import the package generated from its own interfaces: that package imports the Grain package for the interface types used in its proxies and dispatch, so the import is a cycle — the same reason generated artifacts land in their own package that the Grain package does not import (see [codegen.md](codegen.md)). A per-method handle symbol emitted by the generator therefore cannot be named from the code that sets a reminder. The method expression is the only compile-time-checked way to name a method from that code using just the `gor` package and the interface declared in the Grain package, so the handle carries no generated symbol. The generator changes nothing for this.

"One unified entry point" is rejected. Orleans has Grains implement `ReceiveReminder(name)` and switch on the name themselves — that is bringing back the hand-written dispatcher deleted at [step 3](../ROADMAP.md#3-typed-proxy-code-generation), and in user code of all places. gor's selling point is compile-time typing; it must not open a string-dispatch loophole here.

## The stored identifier is the method name

The `method` column holds the method name read off the handle — exactly the string the old string API held. A typed handle changes how the name is authored, not what is stored; the table, the poller, and cross-restart recovery do not change.

A method rename invalidates existing Reminder rows: the stored name no longer
matches a dispatch case, and delivery returns "unknown method". The
identifier follows the method name. A rename is a breaking change to
Reminders.

The hazard is bounded. Where a Grain re-asserts its Reminders on activation or first use, the rename self-heals on the next activation: `Set` overwrites the row by name, including its `method`, so the new name replaces the old. A one-shot Reminder waiting in the table across a rename is the real casualty — it fails once at delivery, the error reaches the configured sink, and the user sets it again.

### Migration

This is a planned v0 breaking change. The public type is `Reminder[T]`, and
the constructor is `NewReminder[T]`. Each `Set` call uses
`gor.Handle(InterfaceName.MethodName)`. `Cancel` takes the Reminder name. The
stored method name and table shape do not change, so existing rows survive a
restart. Only source code needs migration.

## The handle comes from the Binder

Like `State`, `gor.NewReminder[Account](b)` binds the GrainId and storage at
Grain construction. The methods use it directly afterwards.

Not fished out of `ctx`. Hiding runtime capabilities in `context.Value` makes "what this code needs" invisible and forces tests to build the right ctx before they can run. Constructor parameters are explicit; ctx is not.

## When it comes due

Two constructors, enough:

```go
gor.After(d)   // one-shot, fires once after d
gor.Every(d)   // periodic, fires every d
```

`Set` replaces the row with the same name. One Grain has only one Reminder
with a given name. The replacement resets `FirstTickTime` to the new first due
time. It also resets `DueAt` to that same first due time. `Cancel(ctx, name)`
deletes the Reminder.

A one-shot Reminder uses `Period = 0` in its `TickStatus`. A periodic
Reminder keeps its `FirstTickTime` when the poller claims it. The claim reports
the claimed old `DueAt` as `CurrentTickTime`. The poller computes the next
`DueAt` strictly in the future. It does not catch up missed periods.

**Missed windows are not made up.** If the process is down for three periods,
it fires once on return and then tracks to the next future time. Making up
three firings is a trap — what users want is almost never "run everything that
piled up", and how much piles up depends on the downtime, making the behavior
unpredictable. If catch-up is truly wanted, users compute it in the method
from the last execution time.

**Precision is the polling interval.** Persisted Reminders should never promise milliseconds.

## The table

```
reminder(grain_type, grain_key, name, method, first_tick_time, due_at, interval, etag)
```

`first_tick_time` stores the first due time for the current setting. `due_at`
stores the next due time. A zero `interval` means one-shot and maps to
`Period = 0` in `TickStatus`.

The primary key is (GrainType, GrainKey, name). `name` identifies the
Reminder. `method` is the internal method identifier. One method can back
several Reminders with different periods.

## ReminderStore

`ReminderStore` has four operations, aligned with what the poller and the user
need to do:

- **List due** — rows with `due_at <= now`; `now` comes in as a parameter.
- **Claim one row** — CAS with the row's etag, pushing `due_at` to the given next time; a zero time deletes the row. Exactly one claiming node wins.
- **Write one row** — unconditional overwrite; the user's `Set` goes here.
- **Delete one row** — unconditional; the user's `Cancel` goes here.

`ReminderStore` persists `FirstTickTime` with each row and returns it to the
poller when it lists a due row.

**The etag exists only for claiming.** The user's `Set` / `Cancel` carries no etag: the user does not have one anyway, and an explicit reschedule or cancel is his to win. The claim that got overwritten simply delivered one fewer time; at-most-once still holds.

**The next due time is computed by the poller, not the table.** "No catch-up for missed" is policy; the table is only responsible for getting the CAS right. One-shot Reminders use the zero time for "no next" — the same convention as a zero `interval`.

No row-count limit on "list due". Add it when it is actually needed; adding it now decides for a scale that does not exist yet.

## Claim first, then deliver

The poller scans rows with `due_at <= now` and, for each row:

1. **Claim** — CAS to push `due_at` to the next period (delete the row for
   one-shot Reminders).
2. Build the typed request with the generated `newReminderCall` factory.
3. Deliver the ordinary Call through `Runtime.Invoke`, only after winning the
   claim.

The periodic claim keeps `FirstTickTime` and reports the claimed old `DueAt` as
`CurrentTickTime`. It pushes `due_at` to the first time strictly in the future,
not to `due_at + interval`. After three periods of downtime, adding one
interval still lands in the past; the next scan hits the same row again, and
"no catch-up" becomes catch-up.

The reverse order causes repeated firing on a crash. A failure after the claim
can miss one delivery. This is a deliberate trade-off:

**gor promises at-most-once delivery, not exactly-once execution.**

Delivery failures are not retried. Only the user knows whether retrying is safe;
the runtime does not decide for the user — the same stance as on `State.Set()`
conflicts.

**But no retry does not mean silence.** A Reminder delivery has no caller waiting; the error returned by the method is sent by the runtime to the configured error sink, and dropped only when no sink is configured. The runtime does not retry for the user; whether to alert remains the user's decision.

So the configuration needs an error sink:

```go
type BackgroundError struct {
    GrainId GrainId
    Err      error
    Source   ErrorSource
}

type ErrorSource interface {
    errorSource()
}

type ReminderInvocation struct {
    Method string
}

func (ReminderInvocation) errorSource() {}

type Deactivation struct {
    Reason DeactivationReason
}

func (Deactivation) errorSource() {}

func OnError(func(BackgroundError)) Option
```

`ErrorSource`'s unexported method seals the set inside the `gor` package. Code
outside the package cannot implement new sources. Claimed Reminder deliveries
use `ReminderInvocation{Method: ...}`. Deactivation hook failures use
`Deactivation{Reason: ...}`. Callers branch on the source type, not on a
writable string.

**One sink, not one per Reminder, not one per event kind.** It only reports the two kinds of application callback failures with no caller to receive them: claimed Reminder deliveries and normal deactivation hooks. Direct calls return errors to the caller as usual. Polling scans, claim failures, and losing the CAS do not enter the sink — they are operational states of the scheduler and storage, not failures of a known application action.

`Err` is exactly the error the callback got. It follows [errors.md](errors.md): across nodes, only the stable `Code` is usable with `errors.Is`; the event does not add its own `Code` field, nor does it restore error types, fields, or wrapping.

It does no retry, no backoff, no alerting policy — those are the user's business; the runtime only delivers "this failed" into the user's hands. The event carries no reminder name, due time, interval, ETag, or attempt count: after claiming these fields may already be stale, the ETag is not an application decision, and the runtime has no retry model. `timer.Invoker` keeps receiving only GrainId and method.

When unconfigured, these two kinds of errors are dropped. They are the only application callback errors the runtime drops on the user's behalf, and must be written in the docs, not hidden in the implementation.

**A delivery canceled mid-shutdown is not a failure.** At runtime shutdown, in-flight Reminder Calls come back with a cancellation error — the method did not fail; the runtime stopped running. Sending it to `OnError` would report a false alarm to the user on every clean shutdown, and users would have to filter cancellation errors out in their own callbacks. So when the poller's context is already canceled, this error does not go out.

This is not defensive special-casing; it is behavior a test must watch: cancellations during shutdown do not enter `OnError`; other callback errors matching the sink boundary above must enter.

**Claiming must be a CAS, not "read then update".** The pollers of two nodes can scan the same row at the same time; CAS is the only thing that makes one of them lose. This must be right now, not deferred until step 6 — by then, every earlier test would need rewriting.

### Migration

This is a planned v0 breaking change. Existing three-parameter error handlers
change to receive one `BackgroundError`. Reminder deliveries read
`ReminderInvocation.Method`; deactivation hook failures read
`Deactivation.Reason`.

### Gap

The error sink is implemented in the current code. `OnError` receives a
`BackgroundError` with the Grain, original error, and source. The source set
is sealed. The public naming migration remains part of the 0.1.0 API work.

## Don't claim rows that are not yours

In a cluster, every node's poller scans the whole table, but a row's target Grain belongs to exactly one node. Before claiming a row, ask whether it is yours; if not, skip.

Not asking loses deliveries, not duplicates them: when a non-owner wins the claim, `due_at` has already been pushed (the row is deleted for one-shot Reminders), and then the call is rejected by routing — that due time is gone forever, and the owner's poller will not see it next round.

So the poller must ask one more thing: not just "call this method", but "do I own this GrainId". In a single node the answer is always yes — that is not a fake implementation; a single node indeed owns everything.

With an inconsistent view, two nodes may both believe they are the owner; CAS makes one lose, and at-most-once holds. If both believe it is not theirs, this due time is deferred until the view converges; at-most-once still holds.

## Coming due activates the Grain

If the target Grain is not in memory, delivery activates it. This is the point of persisted Reminders: without it, an evicted Grain would never see its next Reminder.

## The poller

One goroutine, driven by the injected `Clock`. `Runtime` must make it exit whether `Close()` or `Kill()` is taken — in `sim`, a goroutine that does not exit makes the whole bubble judged deadlocked.

The poller itself has only "running" and "stopped"; no state enum is invented for it: a two-state state machine is ceremony, not design. It does not distinguish draining from non-draining stops either — it carries no user state, and a delivery canceled halfway still falls within the at-most-once promise.

It gets its own package. `runtime` cannot host it — the poller reads tables, and `runtime` does not import `store`. `gor` should not host it either — that layer only assembles configuration; it does not hold algorithms. So the poller, like `mail`, is a small package: it takes a table interface, a `Clock`, and an interface for initiating calls; `gor` wires the three together.

## A new I/O interface

The Reminder table does not go through `store.Store`. That interface is "read and write one state per GrainId"; scanning due rows, CAS advancement, and row deletion do not fit.

A new interface, shaped by what the poller actually does: list due, claim one row, write one row, delete one row.

It is a new fault source on the step-4 skeleton: the fake implementation injects by seed — scan failures, claim failures, and claim succeeded but the reply was lost. The third is the most important: the claim landed but the poller does not know — exactly where duplicate delivery is most likely.

## Reminder writes are separate from State writes

`reminder.Set()` writes to a different table from `State.Set()`. The Reminder
may be set while the State write is not confirmed.

The two tables stay separate so future backends can use different stores. The
Application must handle this partial result when both changes are needed.

## Invariants

The step-4 skeleton must hold this one:

- **One delivery per due time.** For the same (Grain, name) and the same `due_at`, history contains at most one delivery.

Crashes, claim failures, two pollers scanning at the same time — none of these may break it.

## Minimum tests

The minimum failure, restart, and claim tests must cover these cases:

- A due Reminder is found and delivered after a process restart.
- Two pollers claim the same row at the same time; one claim wins.
- A failed claim does not deliver the Reminder and leaves the row available.
- A process failure after a successful claim and before delivery may miss one delivery.
- A failed Reminder method reaches `OnError`; the runtime does not retry it.
- `Set` replaces by name, resets `FirstTickTime`, and resets the first due time.
- A periodic Reminder after downtime reports the old due time, keeps `FirstTickTime`, and does not replay missed periods.
- A one-shot Reminder reports `Period = 0`.

## Gap

The typed Reminder method handle is implemented in the current code. The
public naming migration to `Reminder`, `ReminderTime`, `NewReminder`, and
`ReminderStore` remains part of the 0.1.0 API work. The `first_tick_time` row
field and the generated typed `newReminderCall` factory are also part of that
work. This design batch does not rename the Go implementation. The method
name is read from the expression once. The table, poller, and restart recovery
use the method-name string as an internal identifier.
