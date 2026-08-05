# Persistence

## Two responsibilities

The `store` package does two different things; don't mix them:

1. **Entity state** — read and write one state per Identity, with optimistic concurrency.
2. **Cluster coordination tables** — the shared tables membership and scheduled tasks need, with CAS required.

Item 2 is the foundation of `cluster` correctness (see [cluster.md](cluster.md)); item 1 only serves business logic.

## Interface

```go
type Store interface {
    Read(ctx context.Context, id Identity) (Record, error)
    Write(ctx context.Context, id Identity, data []byte, expect ETag) (ETag, error)
}

type Record struct {
    Data []byte
    ETag ETag
}
```

`Write` returns `ErrConflict` when `expect` does not match the current ETag in storage. No retries, no merging. A conflict is a business-semantics problem; the runtime has no right to decide for the user.

An empty record (first access) is represented by the zero ETag; passing the zero ETag to `Write` means "require that this record currently does not exist".

**A missing record is not an error.** `Read` on a record that does not exist returns the zero `Record` and a nil error. "Does not exist" and "exists but is empty" are the same thing here — an entity's state is the zero value at first activation anyway, so call sites need not distinguish the two.

## ETag is not optional

In cluster mode, the directory is eventually consistent: during node failures and network partitions, the same key may have one activation on each of two nodes (see [cluster.md](cluster.md)). The only thing preventing the states from overwriting each other is the ETag.

So the ETag is not an "advanced feature"; it is a necessary condition for correctness under this architecture. The API offers no write path that bypasses it.

This matches Orleans' stance: when the official docs discuss the eventual consistency of the default directory, the recommended mitigation is exactly ETag optimistic concurrency at the storage layer.

## Backends

Two goals:

**Embedded** — the single-node default. Candidates: SQLite (`modernc.org/sqlite`, pure Go, no CGO), bbolt, pebble.

Leaning toward SQLite: it satisfies both state storage and coordination tables (coordination needs transactions plus CAS; bbolt's single-writer model can do it too, but SQL expresses multi-row conditional updates like membership more directly), and it has the best operational observability — when something goes wrong, you can look directly with `sqlite3`. The cost is being slower than bbolt/pebble, and the pure-Go SQLite is slower still. The choice will be settled by measured numbers at implementation step 2; it is not decided in advance now.

What to measure is how these two `Store` methods behave under real access patterns, not generic read/write throughput:

- Point-read one record by key.
- CAS-write one record (read-modify-write with ETag check); a single record commits on its own — because `Set()` does not batch.
- Concurrent writes across multiple keys. This one best separates the three candidates: bbolt is single-writer; pebble and SQLite are not.
- How long a cold start takes to open a database with tens of thousands of existing records.

Record the conditions with the numbers: record size, concurrency, machine, WAL on or off. Numbers without conditions are not numbers.

**Postgres** — cluster mode. It is the only choice that satisfies all three: shared across multiple nodes, conditional updates for CAS, and the ops team already has it.

No Redis backend: the atomic conditional updates the membership table needs would rely on Lua scripts on Redis, and Redis' persistence semantics make failures like "lost death votes" hard to reason about.

No cloud-proprietary storage (DynamoDB / Azure Tables and the like). The code Orleans spends on this measures about 26k lines — a major source of its bulk — and the benefit does not hold for this project.

## Gap

The current code provides only the in-memory and SQLite backends. bbolt, pebble, Postgres, and others remain goals or candidates, not currently available implementations.

## How State connects to the runtime

`gor.State[T]` needs to know which Identity it belongs to, which store to write, and what the current ETag is. In the user's struct it is just a field, and the factory `func() Account { return &account{} }` has nowhere to hand these to it.

The solution is for the factory to take one more parameter:

```go
gor.Register[Account](rt, func(b *gor.Binder) Account {
    return &account{balance: gor.NewState[int64](b, "balance")}
})
```

`NewState` registers the cell on the binder; when the runtime activates an entity, it reads the store once according to the registrations and distributes the values to the cells.

Identity comes from here too:

```go
func Self(b *Binder) Identity
```

The binder already holds the Identity — `State` needs it to locate the storage row, and `Schedule` needs it to write the table. `Self` just hands it to the user; it adds nothing.

**It must be a value on the Binder, not entity state.** Storing its own key in state rolls back together with write conflicts, and in a double-activation window an activation would read a stale value — the entity would mistake its own identity.

**Reflection-based struct field scanning and backfilling is rejected.** It would keep the factory as `func() Account` and save users one line, but at the cost of `unsafe` to write unexported fields — and users could not see how the field came alive. The saved line is not worth that price.

### Two more things on the Binder

Time:

```go
func Now(b *Binder) time.Time
```

The binder already holds the injected `Clock` — `Schedule` needs it to compute due times. `Now` just hands it out. If it were not handed out, users would write `time.Now()` in their entities — and that is the very first thing this project forbids.

Calling others:

```go
type Scope interface{ /* sealed: only *Runtime and *Binder implement it */ }

func Ref[T any](scope Scope, key string) T
```

`Ref` originally took only a `*Runtime`, so an entity wanting to call another entity would need the factory closure to capture the runtime object as well, changing the factory signature from `func(b *Binder) T` to `func(rt *Runtime, b *Binder) T`. Cross-entity calls are the most common thing virtual entities do; it should not be the heaviest parameter on the signature.

So the binder holds the runtime, `Ref` takes a sealed interface, and both sides share the name. Sealing — an unexported method in the interface — stops users from implementing it: it is not an extension point, just two shapes of "a place that can resolve entities".

**No third thing on the Binder.** It is the seam between entity and runtime; anything stuffed into the seam must first answer "what happens if it is not stuffed".

## runtime does not import store

`runtime` and `store` are siblings in the architecture diagram; neither imports the other. But the `Binder` must reach both sides: only `runtime` knows the Identity at activation time, and `Store` is injected when `gor` assembles the configuration.

The solution: the factory is called by `runtime` and provided by `gor`:

```go
type Registration struct {
    Factory  func(context.Context, Identity) (any, error)
    Dispatch Dispatch
}
```

`runtime` hands out the Identity and gets back an opaque instance. It does not know the instance carries a Binder, nor that construction read storage once. `gor` is the only package that sees both `runtime` and `store`; the conversion between the two Identity types happens in exactly this one place.

The factory can now return an error — reading storage during activation can fail. This merges with factory panic into the same path: the activation is not established, and the error returns to the caller.

## How conflicts get back to the runtime

When `Set()` hits `ErrConflict`, the activation must be deactivated — but `runtime` does not know `ErrConflict` and should not.

The error cannot be relied on to propagate up. User methods can perfectly well swallow it and return nil, by which time the cached ETag is stale. Deactivation must be independent of how user code handles the error.

So `Set()` immediately sets a flag on the Binder; `gor`'s dispatch wrapper checks it after every call and wraps the result into a shape `runtime` recognizes:

```go
type Discard struct{ Err error }
```

When `runtime` sees `Discard`, it deactivates the activation and returns `Err` unchanged to the caller. It only knows "the entity says it can no longer be used", not why. This shares the path with post-panic deactivation.

## One record per entity

`Store.Read` returns one record per Identity, so all of an entity's `State` cells together encode into one record: a JSON object whose keys are the names given at `NewState`.

This is why the names exist — with one cell the name is indeed redundant, but with a second cell something must distinguish them; a name-free special case for "only one cell" would only make the two shapes look different in storage.

Any cell's `Set()` rewrites the whole record. So the ETag is entity-level, not field-level — exactly the granularity double-activation protection needs: even if two activations modify different fields, one must hit a conflict.

## Encoding entity state

How user state becomes `[]byte`: JSON, not replaceable.

The reason is not that JSON is fast; it is readable — when something goes wrong, you can look directly at what is in storage.

User-injected `Codec` is rejected. An entity's state is one record assembled from multiple cells; the outer container and the values inside the cells must use the same encoding. Making it pluggable has only two outcomes: either the outer layer is always JSON and only the cells go through the codec — a fake codec, since non-JSON bytes cannot fit into a JSON container — or the outer layer is `map[string][]byte`, and then JSON base64-encodes every value, killing readability, which was the entire reason for choosing JSON. Not worth paying that price for a knob nobody asked for.

Encoding for transport between nodes is a separate matter; it will be decided at step 6, see [architecture.md](architecture.md).

No version-tolerant encoding (automatic compatibility when fields are added or removed). Orleans paid 30k lines for it. gor's stance: state structure evolution is handled by users at the application layer — read the old format, write the new one; the runtime does not intervene.

## After a failed write

When `Set()`'s write returns any error, two things happen:

1. **The in-memory value stays unchanged.** Without confirmation of the write, a cell must not pretend the write succeeded — otherwise memory and storage diverge from then on, and the user reads a value that may not exist in storage at all.
2. **Deactivate the activation.** The next call reads from the store again, gets a fresh ETag, and continues.

This is the same line of thinking as panic handling ([runtime.md](runtime.md)): once the instance's in-memory state is untrustworthy, don't keep using it — rebuilding is cheaper than repairing.

**Conflicts and other write errors are not distinguished.** Hitting `ErrConflict` means the ETag is definitely stale; other errors mean the outcome of this write is unknown — storage may have changed, only the reply did not arrive. In both cases the ETag held by this activation is untrustworthy, so the handling should be the same. Separate handling adds a rule and buys nothing but "sometimes it can still muddle through a while longer".

The error is returned to the caller as usual; the runtime does not retry for him — only he knows whether retrying is safe.

## When to write

`State.Set()` writes to storage immediately and waits synchronously for completion.

Batched delayed persistence is rejected: it introduces a window where memory has changed but storage has not — crashes lose data, and the window makes invariant assertions in DST extremely hard to write. If performance is insufficient, users reduce their `Set` count — compute everything in a method, write once — rather than the runtime silently buffering.

## The scheduled task table

```
schedule(entity_type, entity_key, name, method, due_at, interval, etag)
```

This table does not go through the `Store` interface — scanning due rows, CAS claiming, and row deletion do not fit into "read and write one state per Identity". It has its own interface; details in [timers.md](timers.md).
