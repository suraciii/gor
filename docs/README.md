# docs — product spec

This layer describes what `gor` must satisfy: user needs, the API surface, the mental model, the boundaries of responsibility.

Written in product and domain language, for users, assuming the reader does not read the source. Implementation terms (goroutine scheduling, code generation mechanics, storage table structure) belong to [`design/`](../design/README.md).

## Annotation conventions

`docs/` describes the target state, not the current state. Implementation progress lives in [../ROADMAP.md](../ROADMAP.md) — WIP is not marked per document.

When a document and the implementation diverge significantly, the document gets a "Gap" section stating the current state. The body is the spec; the Gap is the footnote.

## Documents

- [vision.md](vision.md) — positioning, three principles, non-goals, relationship to adjacent approaches.
- [programming-model.md](programming-model.md) — the programming model and API shape.
- [errors.md](errors.md) — call errors, stable error codes, cancellation, and the cross-node boundary.
- [example.md](example.md) — the device-shadow example puts entities, state, scheduled tasks, and cross-entity calls on one runnable usage path.
- [compatibility.md](compatibility.md) — v0 and v1 compatibility promises to users, upgrade boundaries, and known limits.
