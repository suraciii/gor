# Vision

## In one sentence

Objects in Go programs that have an identity, hold state, and keep running after a crash should be as easy to write as ordinary structs.

## The problem

People writing stateful services solve the same batch of problems over and over: who owns this user's data right now? Two requests change it at once — what happens? The process dies — what about everything in memory? A scheduled task — will it still fire after a restart?

The usual answer: push state into a database, reread it on every request, add locks or optimistic concurrency, and run scheduled tasks as a separate system. The code is full of "read, check, write, handle the conflict".

`gor`'s answer: seal all of that inside the runtime. You declare an object type and give it a key; the runtime guarantees calls on the same key execute one after another, state survives between calls, and state survives process restarts. You write method bodies, not locks.

## The direction

The direction is narrow and absolute: **a program that needs stateful objects should get them as easily on one machine as it gets a map — and clustering, if it ever matters, is something added later, never the price of getting started.**

Most systems that need stateful objects never grow to dozens of machines. But the existing options all make you pay the distributed price first: install a server, configure a database, run a sidecar. `gor` is the reverse: `import` it and it works, state lands in an embedded store, and one binary is a complete system. Clustering is an optional extension, not the price of admission.

This is not a "start small, scale later" sales line. It is a claim about what the project is *for*. If a user cannot run `gor` on a single node and trust it, the project has failed, regardless of what the cluster can do. Everything else the project spends effort on exists to make that one claim true and trustworthy. Two commitments carry the weight.

### Commitment one — types, not `any`

Comparable Go implementations are typically `Ask(target, message any) (any, error)`. That pushes every error the compiler could catch to runtime: wrong message type, a forgotten case, a renamed field — all of it only shows up when you run.

`gor` requires method signatures to be Go interfaces, so type checking at the call site is exactly like calling a local method. The price is a code generation step, and we accept that price. This commitment exists so the single-node experience is "call a method," not "send a message and pray."

### Commitment two — reproducible tests, not hope

Distributed system bugs concentrate in timing: this message arrived late, this node died at that instant, these two events happened in the wrong order. Testing with real networks and real time only catches the part that happens to show itself when you are lucky.

`gor` has required from day one: all I/O behind interfaces, all time injectable, all components explicit state machines. That way one failure reproduces exactly from one seed. This constraint keeps shaping every design decision — it determines whether this project is worth trusting more than any single feature. It is the reason a single-node user can believe "state survives a crash" without running the crash themselves.

## Where clustering fits

Clustering is an optional extension. It exists, it is shipped, and it is not going to be removed. But it is not the main line, and the project does not ask single-node users to pay for it.

What clustering buys: the same objects, running on more than one machine, with calls routed to whichever node currently owns a key. For a workload that has outgrown one machine, that is the path.

What clustering costs, stated plainly: a failure mode that single-node never has. While the nodes are still settling who owns what — nodes joining, leaving, failing, or split by a network problem — two nodes can each hold the same object active at once. Both accept a write; the one that lands second fails and is returned to the caller as a conflict. On a single node this cannot happen: there is only ever one copy of an object, and calls on it run one at a time. We measured this on a real multi-node run: about one in eight runs surfaced a write that returned a conflict a single node would never return. The caller must retry; the runtime does not.

That is the honest shape of the trade. A single-node user never meets it. A cluster user meets it whenever ownership is changing, and absorbs it. `gor` does not pretend to do better than this; Orleans' default placement has the same shape.

## Non-goals

- **No Orleans compatibility layer.** The inspiration is Orleans, but its API shape carries .NET traces (`Task<T>`, `AsyncLocal`, version-tolerant serialization); carrying them into Go is a burden. When concepts do not match, use a different name.
- **No general-purpose actor framework.** Supervision trees, mailbox policies, behavior switching, actor hierarchies — that is Akka's territory. `gor` does exactly one thing: persistent objects that execute serially by key.
- **No workflow DSL.** No orchestration graphs, no saga syntax. Users write business logic in ordinary Go control flow.
- **No cross-entity transactions.** A call that touches two objects and fails halfway fails halfway — no rollback, no outbox. If two pieces of state must change together, make them one object.
- **No unbounded scaling.** The target is one machine, with a small cluster as an optional extension. Beyond that, use Temporal.

## Relationship to adjacent approaches

In the same problem space, `gor` occupies the "library" cell:

- **Temporal** is the most mature product in this space, but it is a system to deploy (server + database + workers). It fits teams where workflows are the business core.
- **Restate / Rivet** are closest in form to the ideal (single binary), but they are Rust servers; the Go side is only an SDK client.
- **Dapr** has virtual actors, but it is a sidecar model — one more deployment unit and one more network hop.
- **goakt** is the closest library in Go, but its API is `any` in, `any` out, and it has no first-class support for persistent state.

Measured data: [../research/landscape.md](../research/landscape.md) (in Chinese).

## Inspiration and divergence

Orleans' virtual actor model solves a real problem: no explicit create or destroy — a key reference is enough, and the runtime handles activation. This model deserves to be carried over.

But two things deserve honest record:

**Orleans itself is moving elsewhere.** Orleans 10 added journaling and durable jobs; its founder has moved to Temporal and publicly stopped using the word "actor". The market consensus has drifted from "virtual actors" to "durable execution". `gor` follows that consensus — the selling point is "keeps running after a crash", not "actor model".

**Projects in this spot have died.** Orbit is EA's JVM virtual-actor implementation, inspired by Orleans: 1724 stars, rewritten in Kotlin once, abandoned in 2021. It lost to no one technically; it lost in the ecosystem. Reminder: differentiated positioning matters more than feature completeness.
