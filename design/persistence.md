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

Leaning toward SQLite: it satisfies both state storage and coordination tables (coordination needs transactions plus CAS; bbolt's single-writer model can do it too, but SQL expresses multi-row conditional updates like membership more directly), and it has the best operational observability — when something goes wrong, you can look directly with `sqlite3`. The cost is being slower than bbolt/pebble, and the pure-Go SQLite is slower still. The choice will be settled by measured numbers at implementation [step 2](../ROADMAP.md#2-persistent-state); it is not decided in advance now.

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

Encoding for transport between nodes is a separate matter; see [architecture.md](architecture.md).

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

## Durability tiers for state writes

The write path forces every state write fully to storage: a write is not confirmed until it has reached disk. That bounds throughput — it is the number users care about most ([benchmarks.md](benchmarks.md)) — and it leaves a user who can tolerate losing recent writes after a hard crash no first-class way to trade durability for speed short of implementing `Store` themselves.

A durability control gives that user the trade inside the built-in store.

### Two tiers, and what each can lose

Durability is stated in the only terms a user can act on: after a hard crash — power loss, operating-system crash, hard reset — which confirmed writes are gone. A clean restart (the process exits and comes back) is not part of this trade: a clean restart loses nothing at either tier. At Full every commit is already on storage, so this costs nothing extra. At Relaxed the store must flush what the write-ahead log is holding when it closes — a new requirement: today `Close` only closes the handles, and there is no Relaxed tier yet to exercise it. So the Relaxed tier's "clean restart loses nothing" is a guarantee the implementation adds at close time, not one it inherits.

**Full.** A write the call returns is already on storage. A hard crash loses zero confirmed writes.

**Relaxed.** A confirmed write is not forced to storage one at a time. A hard crash can lose the most recent confirmed writes — those that reached the operating system's buffer but were not yet forced to the device. The on-disk state stays intact and readable; it is never corrupted. The loss is not bounded in wall-clock time by `gor`; it is bounded by how much the backend has buffered, which depends on the operating system's writeback. In practice it is on the order of seconds, but `gor` makes no time guarantee.

That is the whole trade: Relaxed buys write throughput by giving up the promise that the last confirmed writes survive a hard crash.

### Why two tiers, and not three

There is no tier that allows the store to come back corrupted. A setting that skips durability checks far enough for a hard crash to leave the store unreadable is not a durability trade — it is a correctness trade that breaks the product promise that state survives a crash. Losing recent writes is a trade a service can knowingly accept; a store that comes back unreadable, losing everything, is not. Such a tier is not offered.

Two tiers, because exactly two behaviors are supported.

### The default is Full

The default is unchanged: Full.

The product rests on "state survives a crash" being true without the user reading a tuning guide. A user who runs the default and loses confirmed state to a power outage has met a broken promise, even if a footnote somewhere permitted it. Relaxed is therefore opt-in: choosing it is the act that says "I accept the trade." Choosing Full leaves the durability behavior exactly as today — no silent change to what survives a crash. The storage layout underneath may still change to isolate the state rows' sync level from the coordination tables; that layout change is a compatibility event, and what it must preserve is nailed in "Old databases" below. The durability option itself is a freely changeable option at 0.0.x and touches neither of the two things 0.0.x keeps stable (error codes; state format) — see [compatibility.md](../docs/compatibility.md).

### Where the tier is chosen

At store-open time, once, for the life of the store. One option; no per-write knob, no per-entity setting, no runtime switching.

This is the smallest model that fits the need. The choice does not change during a run — a service picks the trade at start and lives with it — so it is a property of the store, not of each write. A per-call or per-entity tier would push a durability decision into entity code that has no business making it, and would add a branch to the write hot path for a flexibility nobody asked for.

The tier is an option on the SQLite constructor; omitted means Full, the current behavior:

```go
db, err := store.OpenSQLite("data/gor.db",
    store.WithDurability(store.DurabilityRelaxed),
)
```

`Durability` and its two values (`DurabilityFull`, `DurabilityRelaxed`) live in the `store` package: every backend that implements `Store` shares one type, and the dependency direction (`gor` imports `store`, never the reverse) is what forces it there. Both SQLite constructors take the option — `OpenSQLite` and `OpenSQLiteWithClock` — so a cluster node, which opens with a clock for membership snapshots, sets the state tier the same way a single-node program does. The option does not add a second path to the API: the user names one database, as today, and the store derives the location of any additional database file from it (see "What this means for the SQLite backend"). The in-memory store has no durability tier — it holds nothing across a crash by design, so the option does not apply to it; the tiers are a property of the on-disk backends only. The runtime, the entity, and the write path are unaware of the tier. `Store.Write`'s contract — write the bytes, return the new ETag — is identical at both tiers; only how hard the backend pushes the bytes to storage differs.

### Scope: the state table only

The tier applies to the table that holds entity state. It does not apply to the coordination tables the runtime keeps alongside it in the same database:

- **The scheduled-task table stays at Full.** Delivery promises at most one firing per due time ([timers.md](timers.md)), and that promise rests on the claim step — the row advance that marks a due time as taken — being on storage before delivery proceeds. If that claim could be lost to a hard crash, the due time would look untaken on restart and fire again: a duplicate, which is exactly what at-most-once forbids. The schedule table is not subject to the durability trade.
- **The membership table stays at Full.** It exists only in cluster mode, and the cluster already treats the shared table as eventually consistent: a lost recent heartbeat, vote, or death declaration only slows reconvergence and breaks no correctness invariant ([cluster.md](cluster.md)). But the cluster is an optional, parked extension, and there is no measured reason to widen its failure envelope. Keeping it at Full leaves the cluster's story untouched.

This is the product framing made precise: durability is a property the user trades for *their own* state, not for the bookkeeping that holds the runtime's guarantees together.

### What this means for the SQLite backend

The built-in store runs SQLite in WAL mode. The tier maps to SQLite's per-database sync behavior: Full keeps a sync on every commit; Relaxed syncs only when the write-ahead log is checkpointed back into the main database.

The mapping is safe to offer because of WAL mode specifically: in WAL mode, the relaxed sync level does not corrupt the database on power loss — only recent, un-checkpointed commits can be lost. That is exactly the Relaxed tier's stated semantics; the store is never left unreadable. This is also why the corruption-allowing tier was rejected above: offering it would require stepping outside WAL's safety, and the trade would stop being "lose recent writes."

Because SQLite's sync level is set per database file, not per table, running the state rows at a relaxed tier while keeping the coordination tables at Full means the implementation keeps them in separate database files (or otherwise isolates their sync settings). One shared database under one pragma cannot express "state rows relaxed, schedule rows full." The contract above — the state tier follows the user, the coordination tables are always Full — is what the implementation must satisfy; the file layout is the implementer's choice.

Putting the tables in separate files does not break atomicity that existed before. State writes, schedule writes, and membership writes never shared a transaction: the schedule and membership tables have their own interfaces, and a state `Set` and a schedule `Set` are deliberately not atomic ([timers.md](timers.md)). The split changes where each table lives, not whether any two of them commit together.

On disk the store may now hold more than one database file, each carrying its own `-wal`/`-shm` sidecars. Backups and direct `sqlite3` inspection must cover every file the store creates, not just the path the user named — copying only the main file leaves the write-ahead log behind, and the recovered state is stale or torn.

### Old databases

Today the store keeps the state, schedule, and membership tables in one database file. Isolating the state rows' sync level from the coordination tables means the state rows move to their own database, so an existing database written by an earlier 0.0.x is read into the new layout on first open. The constraint is fixed, not optional: every confirmed state row must come through the move — nothing lost, and the store stays readable. This is not a new promise; it is the 0.0.x promise that a later 0.0.x reads state an earlier 0.0.x wrote, applied to the layout change ([compatibility.md](../docs/compatibility.md)).

The migration is the store's job, done once on first open of an old database, not the user's; the user does not hand-move rows or convert formats. Whether the implementation keeps the single-file layout when the tier is Full and splits only at Relaxed, or splits uniformly regardless of tier, is the implementer's choice — but whichever it is, the constraint above holds: confirmed state survives the upgrade, and the user passes one path either way.

### Relationship to ETag and optimistic concurrency

Relaxed durability does not weaken the optimistic-concurrency guarantee. The ETag's job is to stop a second writer silently overwriting the first during a double activation ([cluster.md](cluster.md)); that is a property of the read-then-write CAS, not of how hard each write is pushed to storage. After a hard crash at the Relaxed tier, the surviving record still carries a consistent, monotonically increasing ETag — it is simply the record as of an earlier commit. The reason is the WAL semantics the Relaxed tier rests on: a power loss can roll back transactions that reached the write-ahead log but were not yet synced, but it never applies a partial or reordered commit, so the record on disk is always one complete earlier version, never a torn one. The ETag on that version is intact, and the next write continues the counter from there; no overwrite goes undetected.

What does change is the reach of the product promise. "Confirmed state survives a restart" holds at both tiers — a restart is clean, and the store flushes on close. "Confirmed state survives a hard crash" holds only at Full; at Relaxed the most recent confirmed writes may be gone. The docs state the trade in exactly those terms.

### The simulation does not model the tier

The durability tier is a performance dimension, not a correctness one, and the simulation does not model it.

Within one run the two tiers are indistinguishable: a write the call confirms is visible to the next read at both. The difference appears only on a hard crash, and the simulation's crash (`Kill`) is a process crash that keeps the on-disk store — it does not model a power loss dropping un-flushed buffers. Modeling power-loss semantics would mean inventing lossy-commit behavior inside the fake store for a code path `gor` does not have: `gor` cannot tell a power loss from a clean restart at runtime, and its restart logic already handles "the state on disk is older than the last write" by reactivating from whatever survived. There is no branch in `gor` that reacts to a lost commit, so there is nothing for the simulation to exercise.

What the simulation must and does cover is the storage seam's correctness-relevant faults — write failures and "the write landed but the reply was lost" ([testing.md](testing.md)). Those are the seams the tier sits behind; the tier is below them.

### Benchmark

The state-write baseline is recorded at each tier, because a single number would hide the only thing the tier exists for. Both numbers are measured on real disk: on tmpfs the sync that separates the tiers is a no-op, so both tiers measure the same fake-fast number and the comparison is void ([benchmarks.md](benchmarks.md)). The relaxed number is expected to be materially below the full-durability baseline; the measurement records by how much.

### Gap

The durability control is not implemented. `store.OpenSQLite` hardcodes full durability for the single shared database that holds the state, schedule, and membership tables together and exposes no option to change it; the separation of the state database from the coordination databases, the flush on close the Relaxed tier requires, and the one-time migration of existing databases are also not done. Everything in this section is the target, not the current code.

## The scheduled task table

```
schedule(entity_type, entity_key, name, method, due_at, interval, etag)
```

This table does not go through the `Store` interface — scanning due rows, CAS claiming, and row deletion do not fit into "read and write one state per Identity". It has its own interface; details in [timers.md](timers.md).
