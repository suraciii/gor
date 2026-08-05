# Architecture

## Dependency directions

The diagram shows only direct imports of production packages. Solid lines are edges that exist in the current code; `┈┈▶` is a target edge not yet connected.

```
gor ────────▶ runtime ────────▶ mail
 │                └────────────▶ clock
 ├───────────▶ store
 ├───────────▶ timer ──────────▶ clock
 │                  └──────────▶ store
 ├───────────▶ clock
 └───────────▶ cluster ────────▶ clock
                    └──────────▶ store

gor ──────────▶ transport
```

`sim` exists only under the `sim` build tag and depends on production packages to set up test scenarios; it is not part of this production dependency diagram. `cluster` and `transport` are both optional capabilities, and `gor` uses both; `transport` remains its own package.

`cluster` does not import `transport`. It only computes remote ownership; `gor` takes that address and forwards. Deciding "who owns it" and "how to get it there" are two things; making the former know the latter only drags a network stack into ring tests.

`timer` knows only interfaces: a table, a `Clock`, and something that can initiate calls. `gor` wires the `store` implementation and `runtime` onto it.

Dependencies point only downward. `runtime` does not know `cluster` exists, and **does not need to leave any interface for it**.

[Step 6b](../ROADMAP.md#6b-转发)'s routing happens in the `gor` layer: every call first asks the ring who owns this Identity; if it is self, hand to `runtime`; if someone else, forward (see [cluster.md](cluster.md)). `runtime`'s interface does not change one word — it is about "calls on the same key are serialized", unrelated to why this key lands on this node.

`runtime` also does not import `store`: entity state is read and written by `gor` inside the factory closure; `runtime` only hands out an Identity and gets back an opaque instance (see [persistence.md](persistence.md)).

The only thing `runtime` gains because of the cluster is an entry to drop activations by Identity: after a view change, `gor` uses it to drop entities that no longer belong to this node. It shares the idle-eviction path and does not reveal the cluster's existence.

**Single-node mode injects nothing**; `gor` takes the local branch directly. No fake implementation that always returns this node is built to "leave an extension point" — that is living dead code; not one line of it is needed in steps 1 through 5.

## Package responsibilities

| Package | Responsibility | Not its job |
| --- | --- | --- |
| `gor` | Public API, configuration assembly | Any algorithm |
| `runtime` | Activation cache, lifecycle, local directory, request dispatch | Network, storage implementations |
| `mail` | The serial execution queue of a single entity | Knowing what an entity is |
| `store` | State read/write plus the CAS table abstraction and its backends | Knowing entity semantics |
| `timer` | Scan due, claim, deliver (see [timers.md](timers.md)) | Knowing entity semantics |
| `cluster` | Membership table, node state machine, view polling, consistent-hash ring (see [cluster.md](cluster.md)) | Executing entity methods, forwarding |
| `transport` | Byte transport between nodes | The semantics of the encoding format |
| `sim` | Fake network, fake clock, fault injection, invariant assertions | Production code paths |
| `cmd/gorgen` | The code generator | Runtime behavior |

## Three boundary rules

**Execution and decision are separated.** `mail` only makes "these calls run one after another"; it does not decide who should run or where. `runtime` decides. Mixed together, scheduling could not be exhaustively tested on its own.

**All I/O sits behind interfaces.** `store`, `transport`, clocks — no exceptions. This is the hard precondition of DST in [testing.md](testing.md); one violation leaves an entire path impossible to simulate.

**No wall clock.** All time comes through the injected `Clock` interface. `time.Now()` in non-test code is a bug on sight.

## Encoding

Inter-node transport needs encoding. **No self-invented serialization format.**

Orleans has 30k lines of serialization code, mostly for version tolerance (old and new nodes interoperating during rolling upgrades). The Go side does not pursue this capability: `gor` assumes every node in a cluster runs the same version of the binary, and incompatibilities during rolling upgrades are resolved at the application layer through downtime or dual-write schemes.

The cost is explicit: **rolling upgrades to incompatible method signatures without downtime are not supported.** The gain is an entire subsystem not built.

`encoding/json` is chosen, the same story as entity state persistence. The reason is no second serialization story, plus humans can read it directly in production — worth more than performance when debugging cross-node problems.

**No `Codec` interface.** An interface with one implementation is ceremony. When encoding really needs to change, the change is in the few encode/decode lines, not in the shape of an interface.

Encoding happens in the `gor` layer. `transport` moves opaque bytes and does not recognize what is encoded (see [transport.md](transport.md)).

## Structural comparison with Orleans

No one-to-one correspondence is pursued, but the source of the size difference is recorded. Orleans' `src/` measures 274k lines, of which only about 26k genuinely need rebuilding on the Go side (directory + membership + activation + placement + hash ring + scheduler), because:

- Serialization (31k lines) — not done, as said above.
- Streams, transactions, event sourcing, journaling (35k lines) — out of scope.
- Cloud provider code (about 26k lines) — replaced by the embedded store and Postgres backends.
- The `src/api/` baseline snapshot (35k lines) — not implementation code at all.

And the Go side can save even more: the scheduler needs 823 lines of custom `TaskScheduler` in .NET, versus a goroutine plus a channel in Go, about 100 lines. Details in [research/orleans-internals.md](../research/orleans-internals.md) (in Chinese).
