# gor

**A persistent Grain Runtime for Go.** It is embeddable, runs as one Silo,
and is designed for deterministic simulation testing.

> **Status:** single-Silo features are implemented and usable. Cluster
> features are an optional preview. Detailed progress: [ROADMAP.md](ROADMAP.md)

## What this is

A Go library that makes a Grain with a GrainId, State, serialized Calls, and
restart recovery your programming unit. You write Go interfaces and structs.
`gor` handles Activation, Call ordering, persistence, and Reminders. A
future cluster is an optional extension, not the main line.

`gor` is a Go port of the Orleans runtime model. The Go API uses Go forms,
but the product terms and runtime meaning follow Orleans. The main design
rules are in the [design documents](design/README.md):

- The programming model is typed at compile time, not `any` in, `any` out — proxies are generated from Go interfaces ([design/codegen.md](design/codegen.md)).
- One Silo is a first-class product. It needs no sidecar or remote service.
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

`gor` does not provide these in 0.1.0; the boundaries are in
[docs/vision.md](docs/vision.md):

- No source or binary compatibility promise with Orleans.
- No Call Filters.
- No reentrant or interleaved Grain Calls.
- No cluster operation tools or unbounded scale.

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
