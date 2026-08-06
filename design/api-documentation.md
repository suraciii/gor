# Public API documentation

## The decision

Before any version is tagged, gor's public API must have English Go doc comments. They are a contract for users, not implementation comments.

This does not overturn the repository's comment principles. Readers of implementation comments can read the source; if a comment merely restates what the code does, the code boundaries or naming are not good enough. Readers of the public API usually see only the signatures and pkg.go.dev; they cannot see activation, persistence, cancellation, or shutdown paths. Signatures cannot express what they may rely on or what happens when misused. Only the contract can fill that gap.

The contract must not become an explanation of the implementation either. It states only facts a caller decides on. When algorithms, goroutines, table structures, or internal call order need explaining, change the design document or the code; do not stuff it into a doc comment.

This work belongs to the ROADMAP's existing "documentation in English" required item; no separate release process is invented. The rules are set now; the public API comments are completed in the same batch once [step 6c](../ROADMAP.md#6c-probing-and-death-voting) is done and a release candidate forms. They must be complete before the candidate tag. This way nothing is written in Chinese first and translated later, and no API that is still moving gets maintained twice.

Two things are rejected:

- Treating public API comments as the implementation comments AGENTS.md forbids. That would leave pkg.go.dev with only signatures, and users would have to guess persistence and lifecycle semantics.
- Adding a one-line Chinese note to every exported name now and translating at release time. Step 6c will still change public usage paths; every change would mean one more translation and semantic proofreading of two texts, costing more than writing English directly on the candidate API.

The cost is one bounded manual review at the release candidate. It does not require maintaining two full manuals: each comment keeps only the local contract this symbol cannot do without; the full model still has `docs/` as its single authority.

## Language

Public API comments are written in English from the start; no bilingual copies.

pkg.go.dev's default reader is a Go user from the first public tag on, not the repository's current maintainers. English originals let that reader see a quotable, searchable contract directly; translating from Chinese later creates extra translation drift, and every API change would need proofreading twice.

This does not require finishing `docs/` and `design/` early. They are large and their target state is still moving; the ROADMAP is right to put them last. Doc comments are short and follow a settled public declaration and a release-candidate review; their readers and their churn boundaries differ.

## Scope

Distinguish packages first, then judge symbols; "all exported names" cannot be the rule.

| Category | requirement |
| --- | --- |
| `gor`, called directly by users | The package doc and every independently usable entry point get a contract: startup and shutdown, registration and references, state, scheduling, lifecycle, observability, errors, options, and the seams generated artifacts call. |
| Extension packages users implement or pass to `gor`: `clock`, `store`, `transport` | Package doc, interfaces and their methods, constructors, closable resources, errors, state values, and fields that affect implementer correctness all get contracts. |
| `cmd/gorgen` | A package doc for the command stating inputs, artifacts, and failure exits; concrete flags defer to the command help and `design/codegen.md`. The generated `Install` that applications call at startup also gets an English comment. |
| Implementation packages in the architecture: `runtime`, `mail`, `timer`, `cluster` | Each package gets at least a doc stating its responsibility and the "applications must not depend on this directly" boundary. Exporting a name does not automatically make it a supported API; no useless per-name comments. |
| `internal/`, test fixtures, example applications, test facilities under the `sim` build tag only | This spec does not apply. They are not part of gor's public surface. |

`gor` is the architecture's public API and configuration assembly layer. `clock`, `store`, and `transport` appear in its public configuration or interfaces; users must be able to implement or pass them, so they are a supported extension surface. The other production packages, though importable in Go, are not promised for direct application use; the package doc must say so, not imply it by blank space.

A declaration inside a supported package gets its own comment if and only if users need to decide or act on it directly:

- Entry points for construction, configuration, startup, shutdown, calls, registration, persistence, scheduling, or observability.
- Interfaces users implement, and every method whose constraints do not follow from the type signature.
- Errors, states, and constants users branch on, retry on, report, or migrate.
- Types, fields, and methods that change zero values, units, ownership, mutability, encoding boundaries, concurrency, blocking, or lifecycle.
- Seams that generated code or custom implementations must call.

The following cases may go without an **independent** comment: the name and type are already clear within a documented aggregate, and there is no separate default, failure, concurrency, or lifecycle rule. For example, a field with no extra semantics in a completion event, or a value fully defined by an already-documented enum type. Its semantics must still be findable from the enclosing type's or interface's comment.

An independently usable exported name that is not meant for applications must not stay blank to dodge documentation. Either a short comment states it is only for generated artifacts or internal assembly, or the visibility or package location changes. No signature restatements like "`WithClock` sets the clock" to pad coverage.

## What a contract must say

The comment starts from the symbol's name. First say at which step of the caller's flow it is used and what result is observable on success. Then add the facts below where applicable:

1. Call preconditions, allowed values, zero values, and defaults.
2. After failure, cancellation, or close, what the caller may no longer assume; whether side effects that already happened may still exist.
3. Resources and retry responsibilities owned by the caller, the implementer, or the callback, respectively.
4. Concurrency, ordering, blocking, and lifecycle constraints: whether calls can be concurrent, what happens after close, whether a callback may block.
5. Boundaries that change the caller's choices: true only locally, or true cross-node only after encoding.

Plain data names do not have to answer all five mechanically. Conversely, whenever an item changes correct usage, it must not be omitted just because the sentence gets longer. `State.Set`'s persistence failure, `Runtime.Done`'s termination meaning, the store's ETag conflicts, the transport's request-completion boundary — all are contracts of this kind that cannot be guessed from a signature.

## What not to write

A doc comment is not a second user manual. The table below gives each kind of information a single home.

| Content | Home |
| --- | --- |
| The full mental model of entities, identities, calls, state, and scheduled tasks; the complete path of cross-entity calls | `docs/programming-model.md` |
| What may be relied on, version upgrades, and breaking changes | `docs/compatibility.md` and release notes |
| Activation cache, mailbox, CAS tables, polling, membership table, frame format, algorithm trade-offs | The relevant `design/` document |
| Multi-node limitations, protocol details, the full matrix of configuration combinations | `docs/` or the relevant `design/` document |
| Full startup tutorial, code-generation flow, end-to-end examples | `docs/`, `examples/shadow/`, and command help |
| Issues, task numbers, design-document links, historical rationale | Not written; history belongs to git log |

A comment may state one local restriction; it must not restate a whole model just to save the reader one click. It also does not describe goroutine counts, locks, database schemas, retry loops, or future implementation plans. These are neither caller contracts nor stable against implementation rot.

## Runnable examples

gor adds no Go `Example` functions and sets none as a release gate.

A realistic root-package call must at least define an entity interface, generate proxies, build a runtime, install the artifacts, register factories, and then obtain a reference and call. It is not a lightweight example demonstrating one declaration. Inside an `Example`, it would compile and run on every `make test`; any v0 change to artifact shape, startup order, or public signatures would mean maintaining a third call path, plus text results when `Output` is present. `docs/example.md` and `examples/shadow/` already carry the full path; duplicating it would not improve v0.1's contract clarity.

Later, an `Example` is added only for a stable scenario that a local comment cannot explain and that genuinely deserves to run directly on pkg.go.dev. Before adding, all of the following must hold: no dependence on real time, network, or processes; no hidden generation step; runs within the default test constraints; its output expresses a stable observable contract. Otherwise keep it as a documentation snippet or an example application; do not create a test entry that rots.

## Acceptance and later changes

The release candidate's API documentation batch is accepted in this order:

1. Step 6c is done, and the candidate commit's public declarations leave no undecided API shape versus the product docs.
2. List the supported entry points and the aggregate fields intentionally omitted from independent comments per this document's package categories, and review the reasons by hand; no fake metric of "every exported name non-empty".
3. Review the signature-only pages with `go doc .`, `go doc ./store`, `go doc ./clock`, `go doc ./transport`, and `go doc -cmd ./cmd/gorgen`. For implementation packages, confirm the package doc states the no-direct-dependency boundary.
4. Run `make ci`. Comment changes do not alter behavior, but the release candidate must still pass the full gate.

From then on, the same change that adds or alters a supported declaration must update its comment in the same change. A comment left unchanged while public behavior changed is a stale contract; "the implementation already explains it" is not a reason to keep it.

## Gap

The candidate API documentation batch is in place: the supported packages (`gor`, `clock`, `store`, `transport`, and `cmd/gorgen`) carry package docs and per-symbol contracts on their independently usable entry points, and the implementation packages (`runtime`, `mail`, `timer`, `cluster`) carry package docs stating the no-direct-dependency boundary. One borderline case remains: the root `Activation` alias carries no independent comment; its semantics are covered by the documented `Activations()` return aggregate and its self-describing fields, which this document's aggregate exception permits, but it is listed here for the manual review this section's acceptance step 2 prescribes. No Go `Example` functions exist, as prescribed.
