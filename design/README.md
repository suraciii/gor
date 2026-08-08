# design — the design spec

This layer describes **how the system should be implemented**: architectural boundaries, data models, interfaces, technology choices, and trade-offs.

Use the ASD-STE100 writing rules in [`docs/writing-style.md`](../docs/writing-style.md). Use the core terms in [`CONTEXT.md`](../CONTEXT.md). Technical terms are allowed when they make the design clear. Define a term when a new reader may not know it. The division of labor with [`docs/`](../docs/README.md): `docs/` says what must be satisfied; `design/` says how.

## Annotation conventions

The body is the spec, not a description of the current state. Implementation follows [ROADMAP.md](../ROADMAP.md) step by step; later steps' content is already written here, so do not read it as existing in the code.

When a document diverges significantly from the code, it lists a "Gap" section inside the document.

## Contents

- [architecture.md](architecture.md) — package boundaries, dependency directions, what goes where.
- [runtime.md](runtime.md) — activation, directory, lifecycle.
- [scheduling.md](scheduling.md) — serial execution, reentrancy, mailbox.
- [persistence.md](persistence.md) — state storage, CAS, backend choice.
- [timers.md](timers.md) — persisted Reminders: the table, the poller, delivery semantics.
- [cluster.md](cluster.md) — membership, placement, directory consistency.
- [transport.md](transport.md) — byte transport between nodes: frames, connections, multiplexing, and close semantics; the substrate boundary for forwarding.
- [errors.md](errors.md) — stable error codes, the call error envelope, and the cross-node cancellation boundary.
- [request-context.md](request-context.md) — the per-Call Request Context API, encoding, lifetime, and failure rules.
- [codegen.md](codegen.md) — typed proxies generated from Go interfaces.
- [testing.md](testing.md) — unit tests and deterministic simulation tests.
- [simulation.md](simulation.md) — the simulation skeleton: seed, fault injection, crashes, event log.
- [observability.md](observability.md) — minimal runtime observability facts and performance bounds.
- [benchmarks.md](benchmarks.md) — what the performance baseline measures and does not, and the measurement conditions comparable numbers must carry.
- [api-documentation.md](api-documentation.md) — English doc comments for the public API: contract boundaries, scope, example trade-offs, and the acceptance process.
- [release.md](release.md) — version numbers, release thresholds, the manual release checklist, and how release-note blocks are handled.
- [release-0.1.0.md](release-0.1.0.md) — the implementation order, failure matrix, conformance example, and evidence gates for the first announced release.

## Decision records

Important trade-offs are written directly in the relevant document; no separate ADR directory. Each trade-off must state what was rejected and what it costs — a trade-off that records only its conclusion is not a record.
