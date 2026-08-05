# gor

**A persistent, stateful runtime for Go.** A single binary, library-shaped, embeddable, designed for deterministic simulation testing from day one.

> **Status:** single-process features are implemented and usable. Multi-node calls can be routed and forwarded to the node that owns the entity; neighbor failure is decided by direct probing and death voting; errors carry stable codes across nodes. Detailed progress: [ROADMAP.md](ROADMAP.md)

## What this is

A Go library that makes objects with an identity, state, single-threaded execution, and crash recovery your programming unit. You write ordinary Go interfaces and ordinary structs; `gor` handles activation, call serialization, persistence, and scheduled wake-ups. Cross-node distribution is an optional extension being implemented in stages.

The idea comes from Microsoft Orleans' virtual actor model, but this is not a port of Orleans. The trade-offs are recorded one by one in the [ADR and design documents](design/README.md); the three most important:

- The programming model is typed at compile time, not `any` in, `any` out — proxies are generated from Go interfaces ([design/codegen.md](design/codegen.md)).
- Single-node is a first-class citizen, not a degenerate mode of clustering. No sidecar, no external database — `import` it and it works.
- Deterministic simulation testing is an architectural constraint, not a testing technique retrofitted afterwards ([design/testing.md](design/testing.md)). This is the main difference between this project and comparable implementations.

## Why it exists

The Go ecosystem has a gap right in this spot. Put the three conditions — stateful, crash-transparent, embeddable — together:

| | Language | Form | Usable single-node |
|---|---|---|---|
| Temporal | Go | separate server + workers | needs a server and a database |
| Restate / Rivet | Rust | single-binary server | yes, but not a Go library |
| Dapr | Go | sidecar process | adds one deployment unit |
| goakt | Go | library | yes, but the API is `any`-based, and maintenance is concentrated on a single person |

Measured details: [research/landscape.md](research/landscape.md) (in Chinese).

## What it does not do

`gor` explicitly does not pursue these; the reasons are in [docs/vision.md](docs/vision.md):

- No Orleans API compatibility layer, and no one-to-one correspondence of concepts.
- No general-purpose actor framework (no supervision trees, mailbox policies, or behavior switching — the Akka-style capabilities).
- No workflow DSL or orchestration graphs.
- No "unbounded horizontal scaling". The target scale is a single machine to a small cluster.
- No cross-entity transactions. A call that touches two entities and fails halfway fails halfway — `gor` gives no rollback and no outbox. If you need atomicity, make them one entity.

## Documentation

- [docs/vision.md](docs/vision.md) — positioning, principles, non-goals. Read this when judging whether a change is aligned with the direction.
- [docs/programming-model.md](docs/programming-model.md) — the user-facing programming model and API shape.
- [design/](design/README.md) — architecture, subsystem designs, technical trade-offs.
- [research/](research/README.md) (in Chinese) — the measured evidence behind those decisions (Orleans source-code measurements, ecosystem landscape, Go-side capability boundaries).
- [ROADMAP.md](ROADMAP.md) — MVP slicing and acceptance criteria.
- [examples/shadow/](examples/shadow/) — a runnable device-shadow service. The API friction it surfaced is recorded in [FINDINGS.md](FINDINGS.md).

## Development

```bash
make test        # unit tests
make sim         # deterministic simulation tests
make gen         # generator end-to-end tests
make net         # transport tests over real TCP
make lint        # vet + staticcheck
```

Requires Go 1.25 or later — `testing/synctest` only became GA in 1.25, and it is the foundation of the testing strategy.

## License

MIT, see [LICENSE](LICENSE).
