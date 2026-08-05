# Deterministic constraint checks

## Purpose

Wire the deterministic testing constraints that a syntax tree can judge precisely into `make lint`. The checker inspects only source facts; it does not try to infer "all I/O is behind interfaces" or "components are explicit state machines" from call graphs or control flow — those two stay with human review.

## Criteria

The checker walks the repository's Go source files, skipping Git metadata and vendored external source. File path, line, and column are printed with every diagnostic.

In production code, using any of the following functions directly after importing `time` is a violation:

- `Now`
- `Since`
- `Until`
- `After`
- `Tick`
- `NewTimer`
- `NewTicker`
- `Sleep`

The only exception is `clock.Real`'s `Now` and `NewTicker` implementations in `clock/clock.go`; they are the real implementation of the injected clock and must connect to the standard library directly. The exception matches file, receiver, and method name together; it does not relax the whole `clock` package.

In test code, `time.Sleep` and `t.Skip`, `t.Skipf`, `t.SkipNow` are all violations. Tests should express synchronization and failure with channels, `testing/synctest`, or explicit failure assertions.

Importing `golang.org/x/sync/singleflight` anywhere is a violation: it waits with a mutex, which cannot express durable blocking in `testing/synctest`.

The checker handles default import names, aliased imports, and dot imports; it checks direct calls and function-value uses alike, so renaming or stashing a function value cannot bypass the rules. It resolves `//go:build` and `// +build` with `go/build/constraint`; it does not guess build targets from file names.

## The sim fixture boundary

`time.Sleep` in default library source has no exception; even `time.Sleep(0)` is a violation. Only simulation fixtures under `sim/`, whose build constraints explicitly require `sim` and whose files are not `_test.go`, may use `time.Sleep`. Here `Sleep` represents the injected storage response delay, scheduled uniformly by the `testing/synctest` bubble: the shared fake store belongs to no single node, and tying its delays to a node's `Clock` has no clear semantics, especially once node clocks have offsets.

This boundary is drawn by build target and responsibility, not as a special case for the existing four call sites, and not as a time-API whitelist for the `sim` package: `time.Now`, `time.After`, and the like in sim fixtures are still violations, and `_test.go` under the sim tag still forbids `time.Sleep`, so it cannot become a test synchronization device.
