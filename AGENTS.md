# Agent Instructions

## Project Context

- Treat [ROADMAP.md](ROADMAP.md) as the source of truth for implementation
  progress.
- Treat [CONTEXT.md](CONTEXT.md) as the source of truth for product language.
  Use its terms.
- Work within the current product and compatibility contracts. Do not add
  placeholder, partial, fake, or speculative features.

## Engineering Principles

- Study established products before designing a solution. Reuse proven
  patterns and conventions when they fit the current requirements.
- Choose the simplest design that fully meets the current requirements.
- Grow the system in working layers. Do not trade a working product for
  unfinished complexity.
- Keep modules small and keep different concerns separate.
- Check existing dependencies before adding code or a package. Prefer a
  maintained library when it reduces complexity or improves reliability.
- Make architecture decisions for the long term. Do not create a stopgap that
  is meant to be replaced later.
- Remove obsolete paths. Add compatibility code only when the product contract
  requires it.
- Keep models small. Add only the properties that the current contract needs.

## Architecture Constraints

- Follow the architectural constraints in [design/testing.md](design/testing.md)
  when changing runtime, store, or cluster code.
- Keep all I/O behind interfaces.
- Get time from an injected `Clock`.
- Use explicit state machines for components with concurrent behavior.
- Use channels for waits across calls. Do not use mutexes for this purpose.
- Keep the deterministic simulation foundation ahead of new cluster work.

## Documentation

- Write or update the product or design spec before implementation.
- Use `docs/` for product requirements and user language.
- Use `design/` for technical design.
- Use `research/` for measured evidence.
- Write repository documents in the simple English defined by
  [docs/writing-style.md](docs/writing-style.md).
- When a document differs from the implementation, add a clear `Gap` section.

## Collaboration

- Work in a separate worktree.
- Work in reviewed batches. Stop after each batch and report the result.
- Keep changes within the assigned files and preserve unrelated changes.
- Write commit messages in simple English. State the actual change.
- Before handoff, inspect the diff and report the verification result.

## Verification

- Before handoff, run the repository CI gate: `make ci`.
- Report failed tests, missing tools, and environment limits. Do not hide them.
