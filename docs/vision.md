# Vision

## In one sentence

Objects in Go programs that have an identity, hold state, and keep running after a crash should be as easy to write as ordinary structs.

## The problem

People writing stateful services solve the same batch of problems over and over: who owns this user's data right now? Two requests change it at once — what happens? The process dies — what about everything in memory? A scheduled task — will it still fire after a restart?

The usual answer: push state into a database, reread it on every request, add locks or optimistic concurrency, and run scheduled tasks as a separate system. The code is full of "read, check, write, handle the conflict".

`gor`'s answer: seal all of that inside the runtime. You declare an object type and give it a key; the runtime guarantees calls on the same key execute one after another, state survives between calls, and state survives process restarts. You write method bodies, not locks.

## Three principles

### 1. Usable single-node, or not usable

Most systems that need stateful objects never grow to dozens of machines. But the existing options all make you pay the distributed price first: install a server, configure a database, run a sidecar.

`gor` is the reverse: `import` it and it works, state lands in an embedded store, and one binary is a complete system. Clustering is an optional extension, not the price of admission.

### 2. Types are for people, not for the runtime

Comparable Go implementations are typically `Ask(target, message any) (any, error)`. That pushes every error the compiler could catch to runtime: wrong message type, a forgotten case, a renamed field — all of it only shows up when you run.

`gor` requires method signatures to be Go interfaces, so type checking at the call site is exactly like calling a local method. The price is a code generation step, and we accept that price.

### 3. Correctness comes from reproducible tests, not hope

Distributed system bugs concentrate in timing: this message arrived late, this node died at that instant, these two events happened in the wrong order. Testing with real networks and real time only catches the part that happens to show itself when you are lucky.

`gor` has required from day one: all I/O behind interfaces, all time injectable, all components explicit state machines. That way one failure reproduces exactly from one seed. This constraint keeps shaping every design decision — it determines whether this project is worth trusting more than any single feature.

## Non-goals

- **No Orleans compatibility layer.** The inspiration is Orleans, but its API shape carries .NET traces (`Task<T>`, `AsyncLocal`, version-tolerant serialization); carrying them into Go is a burden. When concepts do not match, use a different name.
- **No general-purpose actor framework.** Supervision trees, mailbox policies, behavior switching, actor hierarchies — that is Akka's territory. `gor` does exactly one thing: persistent objects that execute serially by key.
- **No workflow DSL.** No orchestration graphs, no saga syntax. Users write business logic in ordinary Go control flow.
- **No unbounded scaling.** The target scale is a single machine to a small cluster. Beyond that, use Temporal.

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
