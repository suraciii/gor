# Programming model

> This document describes the target API. Implementation progress: [../ROADMAP.md](../ROADMAP.md).

## Core concepts

Only three.

**Grain** — a stateful object with a GrainId. You write a Go struct plus a
set of methods. The Runtime guarantees Calls for the same Grain run
serially.

**GrainId** — a GrainType plus a GrainKey. `Account("alice")` and
`Account("bob")` are two different Grains. `Account("alice")` always names
the same Grain. No create or delete call is needed. The Grain starts at its
first Call, may leave memory after idle time, and keeps State in the store.

**Call** — calling a method through an interface. The caller does not know and does not care whether the target is in this process or on another node.

## Declaring a Grain

Write the interface first:

```go
//gor:grain
type Account interface {
    Deposit(ctx context.Context, amount int64) (int64, error)
    Balance(ctx context.Context) (int64, error)
}
```

The first parameter of an interface method must be `context.Context`; the last return value must be `error`. Parameters and return values in between are free-form. A method that does not comply is a generation-time error that names the line.

### Generation prerequisite

The `//gor:grain` marker says this interface gets typed Calls generated for
it. Add the generator to your module once. Run it when a marked interface
changes, before you build. See [../design/codegen.md](../design/codegen.md).

Every Runtime must install the generated output at startup before Grains can
be registered or Grain References can be obtained.

Then write the implementation:

```go
type account struct {
    balance gor.State[int64]
}

func (a *account) Deposit(ctx context.Context, amount int64) (int64, error) {
    if amount <= 0 {
        return 0, errors.New("amount must be positive")
    }
    v := a.balance.Get() + amount
    if err := a.balance.Set(ctx, v); err != nil {
        return 0, err
    }
    return v, nil
}

func (a *account) Balance(ctx context.Context) (int64, error) {
    return a.balance.Get(), nil
}
```

The interface, implementation, and registration live in the Grain package.
The factory uses the unexported `account` type, so the registration stays in
that package.

```go
func Register(rt *gor.Runtime) error {
    return gor.Register[Account](rt, func(b *gor.Binder) Account {
        return &account{balance: gor.NewState[int64](b, "balance")}
    })
}
```

`b` is handed in by the runtime; it connects the state cells to the store. Apart from that, the factory is an ordinary constructor.

No locks in method bodies, because none are needed — a second call on the same key is never running at the same time.

## The Grain knows its GrainId

The struct can also keep its GrainId:

```go
type account struct {
    id      gor.GrainId
    balance gor.State[int64]
}

func Register(rt *gor.Runtime) error {
    return gor.Register[Account](rt, func(b *gor.Binder) Account {
        return &account{
            id:      gor.Self(b),
            balance: gor.NewState[int64](b, "balance"),
        }
    })
}
```

The GrainId is useful for logs, business data, and Calls to another Grain.
The `alice` key can be a user name.

The GrainId is not State. It is not stored as Grain State. It does not change
when the Grain leaves memory and starts again. Two Activations for one
GrainId have the same GrainId.

## The Grain reads time

The `Binder` is given to the factory once, at activation. If method bodies need it, keep it in the factory — registration shaped as in the previous sections:

```go
type device struct {
    b       *gor.Binder
    reading gor.State[reading]
}

func Register(rt *gor.Runtime) error {
    return gor.Register[Device](rt, func(b *gor.Binder) Device {
        return &device{b: b, reading: gor.NewState[reading](b, "reading")}
    })
}

func (d *device) Report(ctx context.Context, value float64) error {
    next := d.reading.Get()
    next.ReportedAt = gor.Now(d.b)
    ...
}
```

Do not use `time.Now()`. Time read by a Grain must come from the Runtime.
Tests must control time, and a future Silo may have a different clock.

## One Grain calls another

The same function as calling from outside, with a different first argument:

```go
gor.Ref[Workshop](d.b, workshopID).DeviceOnline(ctx, deviceID)
```

Outside, the caller holds the Runtime. Inside, the Grain holds the `Binder`.
The factory needs only `func(b *gor.Binder) T`.

Cross-Grain Calls are part of the virtual Grain model. They use the same
typed reference as a local Call.

## Calling a Grain

```go
acct := gor.Ref[Account](rt, "alice")
balance, err := acct.Deposit(ctx, 100)
```

`acct` has type `Account`. A wrong argument type or a missing method is a
compile error. This is the key difference from `any`-based APIs. See
[../design/codegen.md](../design/codegen.md).

### Cluster calls and deployment limits

Call syntax does not change, but three things matter in a cluster:

**Branchable errors must have stable error codes.** Declared codes are checkable with `errors.Is` both locally and across nodes. Undeclared codes leave only displayable text across nodes — no branching on text, type, or fields. Full contract: [errors.md](errors.md).

**Cancellation does not cross nodes.** In a local call, canceling `ctx` cancels the method body's `ctx` too. Across nodes, the caller gets its own `ctx.Err()` first, and the method on the other side keeps its own context and may run to completion. Full boundary: [errors.md](errors.md).

Arguments and return values go through JSON across nodes, so they must be JSON-encodable types. Local calls skip this pass — when the same method works locally and blows up cross-node, this is usually where it comes from.

**An incompatible change cannot ride on a rolling, no-downtime upgrade within one cluster.** Nodes in a cluster must run mutually compatible application versions. When method signatures or state formats are incompatible, the application decides between a release with downtime and arranging dual writes itself.

## Call outcomes and ordering

One Grain processes Calls in a queue. When the queue is full, new Calls are
rejected for overload. The method does not start, and State does not change.

Timeout or cancellation means that the caller stopped waiting. The method
may have started and may have changed State. A delivery error after a Call
was sent has the same unknown result. The caller cannot know if the Business
Action ran.

A method panic returns an error and discards the current Activation. Queued
Calls that did not start also return errors. The Runtime does not replay
them. The next Call builds a new Activation from confirmed State.

While a Grain handles one Call, it does not start a second Call. A Call cycle
is detected and fails instead of waiting forever. The Runtime does not retry
the Call. The Application decides whether a Safe Repeat is valid.

Calls from one caller to one Grain, sent locally in sequence, execute in issue
order. A future cluster does not promise network arrival order.

## State

`gor.State[T]` carries State. `Get()` reads the current value. `Set()` writes
and persists it.

`Exists()` tells whether confirmed State is present. It is different from
reading a present value that contains the type's zero value. `Clear()`
removes confirmed State. After `Clear()` succeeds, the next Activation sees
the State as absent.

A Grain can have several named State values. They are stored as one Grain
record, so any State write updates the Grain version.

**When State holds a map or slice, `Get()` returns that instance, not a copy.**
The change is persisted only after `Set()`. After the Grain leaves memory, an
unsaved change is lost.

Every `Set()` tries to persist immediately. Only success confirms the new
value. On failure, the Runtime keeps the last confirmed value and discards
the current Activation. The next Call reads State again.

Multiple `Set()` calls in one method are separate State writes. An earlier
write may succeed before a later write fails. Keep one business change in one
State update when that result is required.

State must be JSON-encodable. The Application owns State format changes.

In a future cluster, the Runtime may have two Activations for one Grain while
ownership changes. Both may accept a Call. The State version check rejects
the old write instead of silently replacing newer State. The caller receives
a conflict and decides whether to retry.

This behavior follows the Orleans model. A single Silo has no ownership
change, so this cluster conflict does not occur there.

## How durable a state write is

A state change a call returns is confirmed. How much a confirmed change survives a crash is a setting you choose at startup.

Two levels:

- **Full** — the default. Every confirmed change is already on disk when the call returns. If the machine loses power or the operating system crashes, you lose nothing that was confirmed.
- **Relaxed**. Confirmed changes are not forced to disk one at a time. A normal restart — the process exits and comes back — loses nothing. A power loss or an operating-system crash can lose the most recent changes; what is already on disk stays intact and readable, never corrupted.

The trade is throughput. Forcing every write to disk costs time; most services can tolerate losing the most recent changes after a hard crash, and Relaxed lets those services change state faster.

Relaxed touches Grain State and nothing else. Reminders still fire at most
once after a crash. Future cluster ownership data is unaffected.

If you do not choose, you get Full. The mechanism behind the trade and its exact limits are in the [persistence design](../design/persistence.md).

## Reminder

State connects to the store via `gor.State[T]`; a Reminder uses the Binder in
the same way:

```go
type account struct {
    balance  gor.State[int64]
    reminder gor.Reminder[Account]
}

func (a *account) Open(ctx context.Context) error {
    return a.reminder.Set(ctx, "monthly-interest", gor.Every(30*24*time.Hour), gor.Handle(Account.ApplyInterest))
}

func (a *account) ApplyInterest(ctx context.Context, tick gor.TickStatus) error { ... }
```

A Reminder is persistent. After a process crash, a due Reminder can still
run. If the Grain is not in memory, the Runtime starts its Activation.

The Reminder is typed to the Grain interface. The Reminder method uses a
method expression, so a typo or rename is a compile error. The Runtime stores
the method name, not a function value. The method takes `ctx` and
`gor.TickStatus`, and returns `error`.

It is not `time.AfterFunc`. It does not promise millisecond precision. It
does not replay every tick missed during downtime.

One Grain has at most one Reminder with a given name. Setting the same name
again changes that Reminder.

A Reminder can be one-shot or periodic. Cancellation removes it. A one-shot
Reminder is delivered at most once when due.

A Reminder promises at-most-once delivery. It does not promise exactly-once
method execution. The Runtime claims the due time before delivery. A crash
between these actions can miss the Call. A failed method is not retried; its
error goes to the background error sink.

A State change and a Reminder change are separate Runtime actions. The
Application must handle a partial result when both actions are needed.

## How a Grain starts and leaves

A Grain can initialize when its Activation starts. If initialization fails,
that Call fails and the next Call builds a new Activation.

A Grain can run a deactivation hook before it leaves. The hook receives the
reason: idle, ownership lost, normal shutdown, or an untrusted Activation.

The hook cannot prevent deactivation. A graceful stop waits for a hook that
has started. An abrupt stop does not start new hooks. A hook that has started
is not force-aborted.

## Failures nobody is waiting for

Two Application actions can fail with no caller waiting: a claimed Reminder
Call can fail, or a deactivation hook can fail. The Runtime can send both to
a background error sink.

Each event gives the GrainId, the original error, and a source. A Reminder
event gives the method name. A deactivation event gives the leave reason.

Errors still follow the [Errors and cancellation](errors.md) section. Across nodes, only declared stable codes are usable for business branching; error text is for display and logging.

The sink does not retry, back off, or alert. Reminder delivery is at-most-once
by design. The Application owns any Safe Repeat behavior.

The handler must read the Reminder method or deactivation reason from the
event source. It must not infer the source from a method name.

### Gap

The background error sink and deactivation reasons are implemented. The
remaining release work is listed in [../ROADMAP.md](../ROADMAP.md).

## Runtime observability

The Runtime provides two kinds of facts. First, it provides a snapshot of
this Silo's active Activations and their queued Calls. It does not aggregate
data for a future cluster.

Second, it provides one event for each completed Call. The event gives the
caller result, duration, GrainType, and method. A canceled Call has one
canceled result even if the method later completes.

Completion callbacks run with the caller. A callback must not block or do
I/O. The Runtime does not aggregate, export, or alert these events.

## Runtime startup

Single Silo, State in a local file:

```go
if err := os.MkdirAll("data", 0o755); err != nil { return err }
database, err := store.OpenSQLite("data/gor.db")
if err != nil { return err }
defer database.Close()

rt, err := gor.New(gor.WithStore(database))
if err != nil { return err }
defer rt.Close()

if err := gorgen.Install(rt); err != nil { return err }
```

`Install` hands generated proxies and dispatch functions to the Runtime.
Without this line, Grain registration and Grain References fail at startup.

A cluster must explicitly hand the runtime the state store, the shared membership table, this node's address, this startup's generation, and the transport:

```go
nodeTransport, err := transport.New(":7373")
if err != nil { return err }

rt, err := gor.New(
    gor.WithStore(stateStore),
    gor.WithMemberStore(memberStore),
    gor.WithNodeAddr(nodeTransport.Addr()),
    gor.WithGeneration(generation),
    gor.WithTransport(nodeTransport),
)
if err != nil {
    nodeTransport.Close()
    return err
}
defer rt.Close()
```

All nodes share `memberStore`; `generation` must be a fresh value on every rejoin at the same address. `Runtime.Close` closes the configured transport. The difference between a single Silo and a future cluster is configuration, not business code.

## The runtime can stop itself

In a future cluster, a Silo can be declared dead by other Silos. After that it
must serve no Grain.

So the runtime provides a signal:

```go
<-rt.Done()   // closed, or declared dead
```

When it closes, the Runtime stops admitting new Grain Calls. Calls issued
after that get a stable stop error. Codes and checks are in [errors.md](errors.md).

The stop signal does not rewrite results for Calls already admitted. A
graceful stop lets started methods finish and rejects queued Calls. An abrupt
stop cancels started methods but cannot force-abort user code that ignores
cancellation.

Your process should exit, or build a new runtime and rejoin. Ignoring the signal does not silently break anything, but the service should not keep advertising itself as available.

### Gap

The Runtime admission boundary is implemented. Calls after stop receive the
stable stop error. Calls admitted before stop keep the result defined by the
stop mode. The code surface still needs the public Grain terminology and the
State and Reminder operations described in this target API.

## Mental model comparison

If you have used other systems:

| Concept | Orleans | Temporal | Restate | gor |
|---|---|---|---|---|
| Stateful object with a GrainId | Grain | — | Virtual Object | Grain |
| GrainId | GrainId | WorkflowId | Object Key | GrainId |
| Persistent state | `[PersistentState]` | Workflow variables | built-in K/V | `State[T]` |
| Scheduled action | Reminder | Timer | — | Reminder |

The table only builds intuition. Semantics are not fully equivalent. The
Orleans Grain model is the reference for `gor`.
