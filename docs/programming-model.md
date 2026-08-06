# Programming model

> This document describes the target API. Implementation progress: [../ROADMAP.md](../ROADMAP.md).

## Core concepts

Only three.

**Entity** — an object with an identity and state. You write a Go struct plus a set of methods. The runtime guarantees calls on the same identity execute serially.

**Identity** — type + key. `Account("alice")` and `Account("bob")` are two different entities; `Account("alice")` always refers to the same one. No creation, no destruction: it exists from the first call, disappears from memory after enough idleness, state stays in the store, and the next call brings it back.

**Call** — calling a method through an interface. The caller does not know and does not care whether the target is in this process or on another node.

## Declaring an entity

Write the interface first:

```go
//gor:entity
type Account interface {
    Deposit(ctx context.Context, amount int64) (int64, error)
    Balance(ctx context.Context) (int64, error)
}
```

The first parameter of an interface method must be `context.Context`; the last return value must be `error`. Parameters and return values in between are free-form. A method that does not comply is a generation-time error that names the line.

### Generation prerequisite

The `//gor:entity` marker says this interface gets typed calls generated for it. You add the generator to your module once, then run it whenever a marked interface is created or changed, before building. The exact commands and where the generated files land: [../design/codegen.md](../design/codegen.md).

Every runtime must install the generated output at startup before entities can be registered or references obtained. The startup example below shows where installation happens.

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

Register:

```go
gor.Register[Account](rt, func(b *gor.Binder) Account {
    return &account{balance: gor.NewState[int64](b, "balance")}
})
```

`b` is handed in by the runtime; it connects the state cells to the store. Apart from that, the factory is an ordinary constructor.

No locks in method bodies, because none are needed — a second call on the same key is never running at the same time.

## The entity knows who it is

```go
type account struct {
    id      gor.Identity
    balance gor.State[int64]
}

gor.Register[Account](rt, func(b *gor.Binder) Account {
    return &account{
        id:      gor.Self(b),
        balance: gor.NewState[int64](b, "balance"),
    }
})
```

This is needed for logging, for using the key as business data (the `alice` in `Account("alice")` is a username), and for calling another entity and telling it who you are.

An identity is not state. It never enters the store, does not change when the entity is evicted and reactivated, and does not roll back on write conflicts. When the same identity is active on two nodes at once, both activations' `id` is the same value.

## The entity reads time

The `Binder` is given to the factory once, at activation. If method bodies need it, keep it in the factory:

```go
type device struct {
    b       *gor.Binder
    reading gor.State[reading]
}

gor.Register[Device](rt, func(b *gor.Binder) Device {
    return &device{b: b, reading: gor.NewState[reading](b, "reading")}
})

func (d *device) Report(ctx context.Context, value float64) error {
    next := d.reading.Get()
    next.ReportedAt = gor.Now(d.b)
    ...
}
```

Do not use `time.Now()`. Time read by an entity must come from the runtime — tests need to control it, and in simulation each node's clock can carry a different offset. This is the same rule the library itself follows.

## One entity calls another

The same function as calling from outside, with a different first argument:

```go
gor.Ref[Workshop](d.b, workshopID).DeviceOnline(ctx, deviceID)
```

Outside, you hold the runtime; inside, the `Binder`. An entity does not capture a runtime object to call others — the factory signature is `func(b *gor.Binder) T`, and that one parameter is enough.

Cross-entity calls are the most common thing virtual entities do. They must be as easy as local method calls, or users will pile logic into one giant entity to avoid them.

## Calling an entity

```go
acct := gor.Ref[Account](rt, "alice")
balance, err := acct.Deposit(ctx, 100)
```

`acct` has type `Account`. A wrong argument type or a nonexistent method is a compile error. This is the key difference from `any`-based APIs; the price is running code generation once, see [../design/codegen.md](../design/codegen.md).

### Cluster calls and deployment limits

Call syntax does not change, but three things matter in a cluster:

**Branchable errors must have stable error codes.** Declared codes are checkable with `errors.Is` both locally and across nodes. Undeclared codes leave only displayable text across nodes — no branching on text, type, or fields. Full contract: [errors.md](errors.md).

**Cancellation does not cross nodes.** In a local call, canceling `ctx` cancels the method body's `ctx` too. Across nodes, the caller gets its own `ctx.Err()` first, and the method on the other side keeps its own context and may run to completion. Full boundary: [errors.md](errors.md).

Arguments and return values go through JSON across nodes, so they must be JSON-encodable types. Local calls skip this pass — when the same method works locally and blows up cross-node, this is usually where it comes from.

**An incompatible change cannot ride on a rolling, no-downtime upgrade within one cluster.** Nodes in a cluster must run mutually compatible application versions. When method signatures or state formats are incompatible, the application decides between a release with downtime and arranging dual writes itself.

## Call outcomes and ordering

One entity processes calls in a queue. When the queue is full, new calls are rejected for overload outright: the method never starts, and state does not change.

Timeout or cancellation only means the caller stopped waiting. The method may have started, may even have changed state; a cross-node call hitting a post-send network error is the same. Callers cannot tell from this error whether the method ran. Do not retry these two outcomes as if they were overload rejections.

A method panic makes the call return an error and discards the current instance. Calls already queued but not started also end in error; they are not rerun on a fresh instance. The next call rebuilds the instance from persistent state.

While an entity handles one call, it does not start a second. Call cycles like A calling B and B calling A back fail; waiting does not resolve them. The runtime does not retry automatically: whether retrying is safe and how to avoid duplicate business actions is the caller's judgment.

Calls from one caller to one entity, sent locally in sequence, execute in issue order. Cross-node, that order is not guaranteed; operations with ordering dependencies must express the dependency in business data, not rely on network arrival order.

## State

`gor.State[T]` carries state. `Get()` reads the current in-memory value; `Set()` writes and persists it.

An entity can have several cells, distinguished by name. They are stored as one record, so any cell write updates the whole entity's version.

**When a cell holds a map or slice, `Get()` returns that very instance, not a copy.** Mutating it only counts after `Set()` — mutate without writing, and the value changes in memory but not in the store; after eviction and return, the entity reverts to the old value. Copy-before-mutate is a style choice; persistence depends only on `Set()`.

Every `Set()` tries to persist immediately. Only success makes the value the current persisted value; on failure the last confirmed value is kept and the current instance is discarded. After an error, do not assume the instance is still usable; the next call reads state back.

Multiple `Set()` calls in one method are not a transaction. An earlier write may have succeeded while a later one fails; when the business result must be atomic, the business must organize the related data into one state update.

State must be JSON-encodable. The runtime does not carry applications through state-structure evolution; field additions, removals, or format changes are the application's job — read old formats, write new ones.

Concurrency semantics, stated plainly: in cluster mode, the runtime does not guarantee that only one `Account("alice")` runs in the whole world at any moment. A double-activation window exists during node failures and network partitions. So `Set()` carries an optimistic-concurrency check: on conflict it returns an error instead of silently overwriting.

This is not implementation laziness — Orleans' default directory has the same semantics, and its official docs say so (see [../research/orleans-internals.md](../research/orleans-internals.md) (in Chinese)). In single-node mode this window does not exist.

## Scheduled wake-up

State connects to the store via `gor.State[T]`; scheduled tasks take a cell from `b` the same way:

```go
type account struct {
    balance  gor.State[int64]
    schedule gor.Schedule
}

func (a *account) Open(ctx context.Context) error {
    return a.schedule.Set(ctx, "monthly-interest", gor.Every(30*24*time.Hour), "ApplyInterest")
}

func (a *account) ApplyInterest(ctx context.Context) error { ... }
```

Scheduled tasks are persistent: after a process crash, a task that has come due still fires. If the object is not in memory when the task comes due, it is woken up.

What comes due is a method name, not a function value — after a crash nobody can restore a closure; only the name can be stored. The invoked method takes only `ctx` and returns only `error`.

It is not `time.AfterFunc`: do not expect millisecond precision, and do not expect missed firings during downtime to be made up (it fires once on return, then moves on).

One object has at most one task per name; setting the same name again reschedules it.

Tasks can be one-shot or periodic; after cancellation they are not kept. A one-shot task is delivered at most once when due, then disappears.

Scheduled wake-up promises at-most-once delivery, not exactly-once method execution. The system confirms that the due time was claimed, then delivers the method; a crash between the two can miss this firing. Failed methods are not retried automatically either; the error still goes to the error sink below.

A state change and setting, rescheduling, or canceling a scheduled task are not one atomic business operation. Either side can succeed alone; business semantics that need both must handle this window in the application.

## How an entity starts and leaves

An entity can initialize after it starts serving; when initialization fails, that call fails and the next call rebuilds the entity.

An entity can do final teardown before leaving. It learns whether this leave is due to idleness, the current node no longer owning it, a graceful stop, or the instance no longer being trusted. The application can then tell apart "reclaim local resources", "hand back node ownership", "teardown before process exit", and "handle as a fault".

Teardown cannot prevent the entity from leaving. A graceful stop waits for teardown that has already started to return; teardown should finish promptly. The hook gets a fresh work context with no deadline that is never canceled. Under an abrupt stop or when the node is declared dead, teardown that has not started does not run; teardown that has started is not force-aborted.

## Failures nobody is waiting for

Two application actions can fail with no caller waiting for the result: a claimed scheduled delivery fails, or teardown before the entity leaves fails. The runtime can be configured with a background error sink.

Each event gives the entity identity, the original error, and a clear source. A scheduled delivery gives the delivered action's name; a teardown failure gives the reason the entity left. Sources are not application-conventioned text, so an action name that happens to equal the teardown name cannot be confused with it.

Errors still follow the [Errors and cancellation](errors.md) section. Across nodes, only declared stable codes are usable for business branching; error text is for display and logging.

The sink does not retry, back off, or alert for the application. Scheduled delivery is at-most-once by design; an application that retries must design idempotency and state itself. Poller scan and claim failures are not reported here either.

When migrating an existing application, change the handler that used to receive identity, action name, and error to receive an event, then read the action name or the leave reason from the source. Stop guessing the source from the action name.

### Gap

The background error sink is implemented: each event gives the entity, the original error, and a closed set of sources; scheduled delivery carries the delivered action's name, teardown failure carries the entity's leave reason, sources branch by type instead of text comparison, and the source set cannot grow outside the runtime. Deactivation reasons are implemented: when an entity leaves, it receives one of four reasons — idle, current node lost ownership, graceful stop, or instance untrusted; the reason is fixed when the leave begins and later events never rewrite it; the work context given at leave has no deadline and is never canceled. A graceful stop waits for teardown that has started; abrupt stops and declared-dead nodes skip teardown that has not started and do not wait for teardown that has. Poller scan and claim failures are not reported from this sink; the delivery canceled mid-shutdown is not reported either. Everything else in this section is implemented.

## Runtime observability

The runtime hands the application two kinds of facts. First, a snapshot of this node's current activations: which entities are serving, and how many queued, not-yet-started calls each has. It observes only this node; it does not aggregate for the cluster.

Second, an event per completed call. The event gives the result the caller saw, the duration, and the target's type and method. When the caller cancels, the event records the cancellation as the result; even if the method later runs to completion, there is no second event. A cross-node call is recorded once, at the initiating node; the receiving node does not record it again.

Completion-event callbacks run synchronously with the caller. A callback must not block or do I/O — the delay would land on the caller's own result. The runtime does no aggregation, export, or alerting of monitoring data; applications wire it into existing systems.

## Runtime startup

Single node, state in a local file:

```go
database, err := store.OpenSQLite("data/gor.db")
if err != nil { return err }
defer database.Close()

rt, err := gor.New(gor.WithStore(database))
if err != nil { return err }
defer rt.Close()

gorgen.Install(rt)
```

`Install` hands the generated proxies and dispatch functions to the runtime. Without this line, `Register` and `Ref` fail at startup — not at the first call.

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

All nodes share `memberStore`; `generation` must be a fresh value on every rejoin at the same address. `Runtime.Close` closes the configured transport. The difference between single-node and cluster is configuration, not business code.

## The runtime can stop itself

In a cluster, a node can be declared dead by others. After that it serves no entity — serving with an identity the whole world believes dead only writes data nobody will ever see.

So the runtime provides a signal:

```go
<-rt.Done()   // closed, or declared dead
```

When it closes, the runtime also stops admitting new entity calls. Calls issued after that — from this process or another node — get a reliably identifiable stop error. Graceful stops and abrupt stops use the same stop error. When the cluster declares the node dead, the error says the node stopped serving. Codes and how to check them: [errors.md](errors.md).

The stop signal does not rewrite results admitted earlier. A graceful stop lets started methods finish, rejects queued calls that have not started, and waits for methods and deactivations to end. An abrupt stop and a death declaration cancel started methods and reject the queue, but cannot force-abort user code that ignores cancellation. An admitted call may still finish after the signal closes; a call issued after the signal closes cannot succeed.

Your process should exit, or build a new runtime and rejoin. Ignoring the signal does not silently break anything, but the service should not keep advertising itself as available.

### Gap

This section's admission boundary is implemented: the stop transition is the linearization point of admission; after it, calls from this process or another node get the corresponding stable stop error (`gor.runtime_closed` or `gor.node_dead`). "The stop signal does not rewrite previously admitted results" holds only for graceful stops; `Kill` and death declarations cancel admitted calls, and their results become cancellation errors.

## Mental model comparison

If you have used other systems:

| Concept | Orleans | Temporal | Restate | gor |
|---|---|---|---|---|
| Stateful object with an identity | Grain | — | Virtual Object | Entity |
| Identity | GrainId | WorkflowId | Object Key | Identity |
| Persistent state | `[PersistentState]` | Workflow variables | built-in K/V | `State[T]` |
| Scheduled wake-up | Reminder | Timer | — | Schedule |

The table only builds intuition. Semantics are not fully equivalent; notably, Temporal workflows have a deterministic-replay constraint that `gor` does not — `gor` recovers from persisted state, not by replaying an event log.
