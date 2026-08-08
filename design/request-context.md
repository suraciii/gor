# Request Context

## Goal

Request Context is small data attached to one Call. The called Grain can read
the data while that Call runs. The Grain Runtime does not save the data.

0.1.0 provides data propagation only. It does not provide Call Filters,
tracing exporters, response-context propagation, or persistence.

The design keeps the existing typed Grain interface. Each generated Grain
method already receives a `context.Context` as its first argument. The same
context carries Request Context for local and future forwarded Calls.

## Decision

Use two public functions that take `context.Context`:

```go
func WithRequestContext(ctx context.Context, key string, value any) (context.Context, error)

func RequestContextValue(ctx context.Context, key string) (value any, ok bool)
```

The caller adds one entry by deriving a context:

```go
ctx, err := gor.WithRequestContext(ctx, "trace_id", "trace-42")
if err != nil {
    return err
}

return account.Deposit(ctx, 4)
```

The Grain reads the entry from the context passed to its method:

```go
value, ok := gor.RequestContextValue(ctx, "trace_id")
if !ok {
    return errors.New("trace_id is missing")
}
traceID := value.(string)
```

`WithRequestContext` does not change `ctx`. Setting an existing key replaces
the value in the new context only. Repeated calls can add more entries.

The helper validates the key, value type, and complete context size before it
returns. On validation failure, it returns the original context and an error
matching `gor.request_encode_failed`. It does not create a Call.

`RequestContextValue` returns the canonical value stored in the context. It
returns `(nil, false)` when the key is missing. A stored nil value is different
from a missing key and returns `(nil, true)`.

Canonical means that local and forwarded Calls expose the same Go type for a
value.

## API alternatives

The design considered these shapes:

| Shape | Benefit | Cost | Decision |
| --- | --- | --- | --- |
| Helper functions on `context.Context` | Fits the existing typed method and `Invoker` signatures. Parent contexts stay isolated. Nested Calls can pass the same context. | Callers must pass the context they want to use. | Chosen. |
| A mutable package Request Context bag with `Set`, `Get`, and `Clear` | Short calls after setup. | Go has no safe task-local mutable bag. Concurrent Calls could share or overwrite values. Clear and restore rules would be easy to get wrong. | Rejected. |
| Call options added to every generated method | Makes Call data visible in each method signature. | Changes every Grain interface and generated proxy. It creates a second path beside the existing `context.Context` path. | Rejected. |

The chosen shape keeps one call input, one proxy path, and one isolation rule.
It also leaves method signatures stable when a future transport is added.

## Context lifetime and isolation

Request Context belongs to a context value, not to an Activation or a GrainId.
The value exists from the derived context until that context is no longer used.
The Runtime does not retain it after the Call completes.

The implementation stores an immutable snapshot. Each write copies the entry
map before it changes a key. A parent context therefore keeps its old data.
Different goroutines can derive different contexts from the same parent without
sharing a mutable Request Context map.

The Runtime must create a new incoming context from the transport handler
context and the decoded request data. It must replace, not merge, any private
Request Context value at that boundary. A previous handler or Call must not
leak data into the next Call.

An independent Call has no Request Context unless its caller passes a context
which contains one. Reusing the same derived context for two Calls sends the
same entries on both Calls. This is explicit caller behavior, not propagation
from the first Call.

Ordinary values stored with `context.WithValue` are not Request Context. The
Runtime does not scan or encode arbitrary context values.

## Supported values

0.1.0 supports scalar values only. The value must have one of these built-in
Go types:

| Input type | Canonical value seen by the Grain | Wire type |
| --- | --- | --- |
| `nil` | `nil` | `null` |
| `bool` | `bool` | `bool` |
| `string` | `string` | `string` |
| `int`, `int8`, `int16`, `int32`, `int64` | `int64` | `int64` |
| `uint`, `uint8`, `uint16`, `uint32`, `uint64` | `uint64` | `uint64` |
| `float32`, `float64` | `float64` | `float64` |

The helper normalizes integer and floating point values before it stores them.
Callers must use the canonical type when they type-assert a value. Floating
point values must be finite.

Named types are not accepted, even when their underlying type is supported.
The caller must convert a named value to a built-in type first. `uintptr` is
not supported.

The Runtime does not support maps, arrays, slices, structs, pointers,
functions, channels, `json.RawMessage`, or `[]byte` as Request Context values.
It also does not call a user `MarshalJSON` method. This keeps values immutable,
keeps the wire contract small, and prevents application object graphs from
becoming part of the Call contract.

Keys must be non-empty valid UTF-8 strings. A key is limited to 128 bytes. A
key is an entry name, not a path or a reserved Runtime field. String values
must also be valid UTF-8. This keeps local and forwarded Calls equal because
`encoding/json` replaces invalid string bytes during encoding.

## Encoding and size limits

The existing `encoding/json` Call envelope remains the only transport encoding.
Request Context is an optional `request_context` field in an invoke request.
The field is absent when the context has no entries. Its JSON shape is:

```json
{
  "request_context": {
    "attempt": {"type": "int64", "value": 2},
    "trace_id": {"type": "string", "value": "trace-42"}
  }
}
```

The `null` entry uses `{"type":"null"}` and has no `value` field. The other
types use the type names in the table above. The type tag selects the decode
target before `value` is decoded. In particular, `int64` and `uint64` values
must be decoded directly into those Go types, not through `any` or `float64`.
Normal typed `encoding/json` decoding is sufficient.

The decoder must reject an unknown type, a wrong JSON value for a type, or an
invalid key. It does not need a duplicate-key parser. Repeated object keys
follow the existing `encoding/json` object behavior and do not create a
separate 0.1.0 failure.

`encoding/json` sorts map keys when it creates the canonical bytes. The
complete canonical JSON object for `request_context` must meet both limits:

- no more than 32 entries;
- no more than 4096 bytes, including keys, type names, values, and JSON syntax.

The limit does not include the rest of the Call envelope or method arguments.
The transport's own frame limit still applies to the complete request.

The source checks these limits when `WithRequestContext` derives a context.
The receiver checks them again after decoding. The second check protects the
receiver from a peer which sends a request outside the contract.

No encoding plug-in, custom binary format, or version-tolerant format is part
of 0.1.0. A later wire change needs its own compatibility design.

## Failure boundary and error codes

Request Context must not add a new framework error code. The existing stable
codes keep their current meanings:

| Condition | Boundary | Result |
| --- | --- | --- |
| Invalid key, unsupported value, non-finite float, or local size limit | `WithRequestContext` | Returns an error matching `ErrRequestEncodeFailed`. The parent context is unchanged. No Call is admitted. |
| Source cannot encode the invoke envelope | Forwarding before `Transport.Send` | Returns `ErrRequestEncodeFailed`. The target method does not start. Root admission and release still follow the existing path. |
| Peer sends invalid or oversized Request Context | Inbound request handling after root admission and before activation | Returns `ErrInvalidRequest`. No Activation or method entry occurs. |
| Runtime is closing or dead | Root admission | Returns the existing `ErrRuntimeClosed` or `ErrNodeDead` result. Stop admission wins over request-specific work. |
| Transport fails or the caller context ends after forwarding starts | `Transport.Send` | Returns `ErrTransportFailed` or the original `ctx.Err()`, under the existing unknown-result rules. Request Context does not prove that the target method did not run. |
| Grain method returns an error | Method dispatch | Returns the method error. Request Context adds no error handling and no retry. |

The public `Runtime.Invoke` path still admits before ownership lookup and
forwarding. The inbound handler still admits before it starts activation. A
Request Context check must not create a path around either gate, the mailbox,
call serialization, or cycle detection.

## Local Calls

For a local Call, the generated proxy passes the same `context.Context` to
`Runtime.Invoke`. The root Runtime passes it to the local execution Runtime,
which passes it through activation, the mailbox, and dispatch.

The local path does not encode or decode the Call envelope. The helper already
validated and normalized the Request Context, so local and forwarded Calls
have the same visible value types.

Request Context does not change mailbox capacity, Call order, overload, cycle
detection, or the non-reentrant rule. A queued Call still carries its own
context, but a canceled or rejected queued Call does not enter the method.

## Forwarded Calls

Forwarding keeps the current split in `Runtime`:

1. Root admission succeeds.
2. Ownership selects another Silo.
3. The source encodes arguments and the optional Request Context field.
4. The transport sends the existing invoke request.
5. The target validates the request, derives its incoming context, and calls
   the same local execution Runtime.

The target does not route the already forwarded request again. The Request
Context field is request data only. It is not part of Grain State, Reminder
records, activation identity, or the occupied call-cycle chain.

The response envelope has no Request Context field. A value added by the
called Grain is not returned to the caller. A caller's context is not changed
by a successful reply, a method error, a reply encoding error, or a late reply.

## Nested Calls

A nested Call inherits Request Context only when the Grain passes its incoming
context to the Grain Reference method:

```go
return other.Lookup(ctx, key)
```

The nested Call can add or replace a value by deriving another context. The
parent method's context is unchanged. A nested Call made with a new context
such as `context.Background()` has no Request Context unless the caller adds
one to that new context.

Nested Calls use the same local or forwarding path as an outside Call. Request
Context does not allow reentrancy, change call-cycle detection, or alter the
existing cancellation and unknown-result rules.

## Activation and Deactivation

An Activation is created for the first Call that needs it. That triggering
Call's context is used by the existing activation factory path. Therefore
`OnActivate` may read the triggering Call's Request Context.

Only the triggering Call supplies that context. If another caller waits for
the same Activation, its Request Context is not merged into the activation
and does not run `OnActivate` again.

The Runtime stores no Request Context on the Activation. Each method receives
the context for its own Call. After idle eviction, a later Call can create a
new Activation and its context can be visible to that new `OnActivate`.

`OnDeactivate` receives the existing fresh background context. It never sees
the Request Context of the last Call, including during normal close, idle
eviction, ownership loss, or fault handling.

## Reminders and persistence

A Reminder is a Runtime-created Call. Its persisted record contains the
Reminder schedule and tick data only. It does not contain Request Context.

The poller starts each Reminder Call with an empty Request Context. A prior
outside Call, a prior method, and the current Activation cannot supply values
to a Reminder. The Reminder method may derive a context for a nested Call, but
the Runtime does not save that derived context.

Request Context is not written to Grain State or any Reminder storage. An
Application may deliberately write a value to its own State as normal
Application behavior, but that is not Request Context propagation.

## Cancellation and shutdown

For local Calls, cancellation keeps the existing meaning. The caller may stop
waiting, and a method which already started may continue or may observe the
canceled context. Request Context remains readable while that method runs.

For a forwarded Call, the sender's context bounds only the source forwarding
operation. It is not encoded as a deadline or cancellation frame. If the
sender cancels after the request can be delivered, the target method can still
run and read Request Context. A late response is discarded by the existing
transport path.

Close, Kill, and cluster death keep their current admission and method rules.
Request Context does not keep a Call alive, cancel a target method, or add a
retry. A clean shutdown test must show that a Call cannot use Request Context
to enter after the root admission gate closes.

## Acceptance test matrix

The implementation must add focused tests to the existing unit and fake
cluster test paths. Tests must use the injected clock, fake transport, and
channel or `synctest` coordination. They must not use sleeps or real network
I/O for these cases.

| Test | Setup | Required result |
| --- | --- | --- |
| Local read | Derive a context with every supported value type and call a local Grain. | The Grain reads the same canonical values. Missing keys return `(nil, false)`. Explicit nil returns `(nil, true)`. |
| Parent isolation | Derive two contexts from one parent with different values. Call the same Grain concurrently and call once with the parent. | Each Call sees only its own snapshot. The parent Call has no child values. |
| Nested propagation | A Grain calls another Grain with its incoming context, then with a context which replaces one key. | The first nested Call inherits the values. The second sees the replacement. No parent context changes. |
| Forwarded read | Route a Call to another Silo with all supported values. | The target Grain reads the same canonical values through the normal forwarding envelope. |
| Independent forwarded Call | Run a context-bearing Call, then run a Call with the base context. | The second Call has no Request Context. No value is retained on the target Activation. |
| State and Reminder isolation | Make a context-bearing Call, inspect State and Reminder records, then deliver a Reminder. | No Request Context field is stored. The Reminder sees an empty context. |
| Validation boundary | Try an unsupported value, invalid key, non-finite float, too many entries, and an oversized context. | The helper returns `ErrRequestEncodeFailed`, leaves the parent unchanged, and no factory or method runs. |
| Canonical integer decoding | Forward `int64` and `uint64` values, including values above the exact `float64` range. | The target reads exact `int64` and `uint64` values, not `float64` values. |
| Malformed peer request | Send an unknown type, wrong typed value, invalid key, or oversized `request_context` field directly to the handler. | The handler returns `ErrInvalidRequest` before activation or method entry. |
| Cancellation | Cancel before local delivery, cancel during a local method, and cancel a forwarded send after delivery. | Existing local and forwarded cancellation results remain unchanged. The target forwarded method can still read its Request Context. |
| Admission and shutdown | Fill a mailbox, close the Runtime, and send an inbound request after admission closes. | Queue and shutdown rules remain unchanged. Request Context cannot bypass admission or start a rejected method. |
| Lifecycle boundaries | Start an Activation with one context, make later Calls with another, then deactivate it. | Only the triggering `OnActivate` sees its context. `OnDeactivate` sees none. |
| Response boundary | The called Grain derives a new context before returning. | The caller's context is unchanged and the response has no context data. |

The focused tests prove the feature contract. The implementation batch must
also pass `make test`, `make sim`, `make net`, and the repository `make ci`
gate before the feature can be called complete.

## Gap

The two public helpers, immutable snapshots, scalar normalization, validation,
and finite-float checks are implemented. String values are checked for valid
UTF-8 before storage. Local Calls keep the caller's context; forwarded invoke
requests carry the optional `request_context` field and the receiver validates
and replaces its private snapshot before activation. Typed integer decoding
preserves `int64` and `uint64` values, including exact forwarded boundary
values. Focused tests cover these Request Context behaviors and the existing
runtime and transport tests cover the shared admission, mailbox, cycle,
shutdown, and cancellation rules. The matrix is not claimed to have a
separate Request Context test for every shared-runtime row.
