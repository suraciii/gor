# Errors and cancellation

## Goal

Call outcomes can leave the process. Arbitrary Go `error` objects cannot. Cross-node outcomes are therefore projected onto a stable code and text; no attempt to pack the object back.

The projection rule looks only at whether the error carries a `Code`. It does not whitelist concrete error types. Framework errors carry their own codes where they are produced; application errors are coded by the application; everything else is an opaque error.

## Public types

```go
type Code string

type Coded interface {
	Code() Code
}

func CodeOf(error) (Code, bool)
```

`Code` implements `error` and `Coded`. Applications declare it as a package-level constant:

```go
const ErrWorkshopIDRequired gor.Code = "shadow.workshop_id_required"
```

`CodeOf` returns the only reachable code in the error tree. It walks the error itself, the `Unwrap() error` chain, and every `Unwrap() []error` branch the way `errors.Is` does, collecting the codes of all `Coded` errors found. With exactly one distinct code it returns it; with no code, or more than one distinct code, it returns empty, meaning the error has no determinate code. Merging multiple errors thus stops being a special case: a merge only adds branches to the same tree, and code reachability is judged by the same rule.

A coded error rebuilt on the remote side also implements `Coded`. Its `Is` compares only `Code`. When `ErrWorkshopIDRequired` is the only reachable code in the error tree, the following assertion holds locally and remotely:

```go
errors.Is(err, ErrWorkshopIDRequired)
```

When the error tree has no code, or more than one distinct code, `CodeOf` returns empty, the envelope carries no code, and the source side rebuilds a text-only error; callers cannot match such an error against a code across nodes. This is the full parity scope of `errors.Is`. The remote error is not equal to the original: no arbitrary sentinel is restored, `errors.As` is not guaranteed to yield the original type, and wrap depth, fields, and merged members are not preserved.

## The code space

A valid code has the form `<owner>.<name>`. Both parts consist of lowercase ASCII letters, digits, and underscores, and start with a letter. `owner` is the ownership boundary of the code. Applications choose an `owner` they own; `gor.*` is reserved.

This version's framework code set is sealed as follows:

| Code | Outcome |
| --- | --- |
| `gor.no_owner` | The current view has no routable owner. |
| `gor.node_dead` | The target node has stopped serving. |
| `gor.runtime_closed` | The runtime or the entity's mailbox is closed. |
| `gor.overloaded` | The call was rejected for a full queue before the method started. |
| `gor.type_not_installed` | The target node does not have this entity type. |
| `gor.unknown_method` | The target type has no such method. |
| `gor.invalid_request` | The request's shape or arguments cannot be decoded under the current contract. |
| `gor.persistence_conflict` | The state write hit a version conflict. |
| `gor.persistence_failed` | The state write failed, and it was not a version conflict. |
| `gor.panic` | The factory or the entity method panicked. |
| `gor.request_encode_failed` | The source could not encode the arguments into a call request. |
| `gor.reply_encode_failed` | The return values of a successful call could not be encoded. |
| `gor.transport_failed` | The request, response, or connection failed to transfer; the execution outcome is unknown. |

The framework must not invent `gor.*` codes outside this set for the same outcome. Applications must not use `gor.*`. This version registers no extra mappings for arbitrary error types and derives no codes from error text.

## The call response envelope

The call response becomes:

```go
type errorEnvelope struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message"`
}

type callResponse struct {
	Reply json.RawMessage `json:"reply,omitempty"`
	Error *errorEnvelope `json:"error,omitempty"`
}
```

`Error == nil` means the call succeeded. When non-nil, `Code` may be empty and `Message` is required. An empty code means an opaque error; the receiving end rebuilds a text-only error. A non-empty code rebuilds a `Coded` error so `errors.Is` matches by code.

Call handling runs in this order:

1. Execute the method; first obtain the business error.
2. When the business error is non-nil, project only it into `Error`; do not encode `Reply`.
3. When the business error is nil, encode `Reply`. An encoding failure returns `Error` with `gor.reply_encode_failed`.
4. Last, encode the whole `callResponse`.

Step 2 is the priority rule. Reply encoding is a diagnostic-layer failure and must not replace a business error already obtained. When step 4 fails, there is no sendable call result; treat it as `gor.transport_failed`, which must not override a business error that is already sendable.

Request-encoding failures, `Send` failures, and response-decoding failures also return `gor.request_encode_failed` or `gor.transport_failed`. `gor.transport_failed` does not mean the request was not delivered, the method did not start, or state did not change.

## Local and remote

Local calls do not go through the envelope. They keep the original error object. As long as the error chain declares a `Code`, local `errors.Is` matches it by Go's standard rules.

Remote calls project the error on the server and rebuild it on the source. Projection uses `CodeOf`; rebuilding uses an error that matches only by code. The determinate code is therefore the one error identity both locations share. Text may add context but must not affect any branch.

The framework constructs the table's codes on every public call path, including local ones. Then `errors.Is` does not depend on call location for framework codes either. Internal packages' old sentinels may remain as internal implementation details, but must not be the sole identity of a `gor` public call result.

## Cancellation

`Runtime.invoke` passes the caller's `ctx` to `transport.Send`. When `Send` completes because of that `ctx`, the source returns `ctx.Err()` directly and removes the local pending request.

The receiving handler's context comes from the service lifecycle; it does not derive from the sender's `ctx` and carries no sender deadline. There are no cancellation-frame or deadline fields. Once a request is received, the remote method is canceled only by the remote side's own shutdown, termination, or method logic.

Source-side cancellation therefore has three requirements:

1. The source returns the original `ctx.Err()` and does not wait for the remote side.
2. The remote context is not canceled; the remote method keeps executing.
3. A late-arriving remote result is discarded and not written into the caller's reply.

The caller has no observable "delivered" boundary. It cannot conclude from cancellation, timeout, or `gor.transport_failed` that the remote side did not execute. The rule that a transport failure cannot prove non-execution is independent of the cancellation rules and must be kept.

## What is not done

No arbitrary type registration, no field serialization, no fidelity of error chains or joined structures, no error-code codegen annotations, no cancellation frames, no remote deadline propagation. All of them widen the wire contract without changing the stable code, the one cross-node error identity. Code reachability takes the unique code through a join, but that is not fidelity: the joined members, their count, and their individual texts are not preserved across nodes.

## Gap

The current implementation already uses the error envelope, processes business errors before encoding successful replies, and rebuilds coded errors on the source; public call paths, the cancellation boundary, and the shadow and simulator migrations are covered. Still not provided: arbitrary error type registration, field or error-chain fidelity, joined-structure fidelity, cancellation frames, or remote deadline propagation. These belong to "what is not done"; they are not current gaps.
