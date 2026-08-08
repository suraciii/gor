# docs — product spec

This layer describes what `gor` must satisfy: user needs, the API surface, the mental model, the boundaries of responsibility.

Written in plain English and in product and domain language, for users who do not read the source. The writing rules are in [writing-style.md](writing-style.md). Implementation terms (goroutine scheduling, code generation mechanics, storage table structure) belong to [`design/`](../design/README.md).

The root [CONTEXT.md](../CONTEXT.md) is the source for the core product
language. Use its terms before adding a new synonym.

## Annotation conventions

`docs/` describes the target state, not the current state. Implementation progress lives in [../ROADMAP.md](../ROADMAP.md) — WIP is not marked per document.

When a document and the implementation diverge significantly, the document gets a "Gap" section stating the current state. The body is the spec; the Gap is the footnote.

## Documents

- [writing-style.md](writing-style.md) — the English and ASD-STE100 writing rule for repository documentation.
- [vision.md](vision.md) — product direction, core promises, and boundaries.
- [programming-model.md](programming-model.md) — the programming model and API shape.
- [errors.md](errors.md) — call errors, stable error codes, cancellation, and the cross-node boundary.
- [example.md](example.md) — the device-shadow example puts Grains, State,
  Reminders, and cross-Grain Calls on one runnable usage path.
- [compatibility.md](compatibility.md) — v0 and v1 compatibility promises to users, upgrade boundaries, and known limits.
- [release-0.1.0.md](release-0.1.0.md) — the first announced release contract, supported capabilities, non-goals, and acceptance standard.
