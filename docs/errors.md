# Errors and cancellation

A Grain Call may complete on this Silo or on another Silo. Callers can rely
on the same outcome rules. Location transparency guarantees exactly that.

Location transparency does not preserve in-process error objects. An error's concrete type, fields, wrapping, and implementation details do not become a contract because the call crosses nodes. Calls do not promise exactly-once execution either.

## Stable error codes

When callers need to make decisions based on errors, the application must declare a stable error code. Error codes are the application's public vocabulary, not text for log searching.

An error code is an owner name plus a name, both lowercase ASCII, separated by a dot. Example: `shadow.workshop_id_required`. The name may contain lowercase letters, digits, and underscores. The owner name must be owned by the application. `gor` is reserved as the library's owner name; applications must not declare `gor.*`.

Applications declare their codes as package-level `gor.Code` constants and return the constant, or an error wrapping it, as the method result:

```go
const ErrWorkshopIDRequired gor.Code = "shadow.workshop_id_required"

return fmt.Errorf("workshop ID is required: %w", ErrWorkshopIDRequired)
```

Callers check the constant with Go's `errors.Is`. Error text is for humans, logs, and diagnostics. It is not a branching condition.

gor's own codes are a closed set. The library only uses the published `gor.*` codes. Full list and the meaning of each code: [../design/errors.md](../design/errors.md).

## What the caller gets

| Outcome | Local call | Cross-node call |
| --- | --- | --- |
| Error with a stable code | The original error matches the code. | A new error matches the same code. Text may differ. |
| Error with no determinate code | The original error object is returned as-is. | Only displayable text remains. |
| Caller cancels or times out | The caller gets its own cancellation or timeout error. | Same. The remote side may still be executing. |
| Send, connect, or reply-receive failure | This is a delivery failure. | Same. It cannot prove the remote side did not execute. |

This parity applies only to an error's determinate stable code. An error has at most one determinate code: the only code appearing in its error tree. An error with a determinate code matches by that code both locally and across nodes. An error with no determinate code — the whole tree has none, or has several different ones — leaves only text across nodes. Merged errors follow the same rule: if the merge leaves exactly one code, it counts; several different codes are ambiguous and count as none. Parity does not promise that arbitrary sentinels, concrete types, or `errors.As` behave identically on both sides.

An error with no determinate code can still be displayed, logged, and returned upward across nodes. Callers must not branch on its text, type, fields, or wrapping, and must not infer business state from it.

## Cancellation

Caller cancellation or timeout means the caller is no longer waiting. It does not mean the business action did not happen.

A local call hands the caller's cancellation to the method. A cross-node call carries neither cancellation nor deadline. If the caller's cancellation happens first, the caller immediately gets its own `ctx.Err()`; a method already delivered to the remote side keeps using the remote side's own execution context. The remote side can complete, change state, and produce a result; the result is discarded at the source.

This boundary keeps the caller from knowing whether the request was delivered. To avoid duplicate actions or to compensate, applications must put idempotency keys, state machines, or compensation rules into the business protocol.

## Reply that cannot be encoded

When a method has already returned a business error, that error wins. The runtime does not try to encode the same call's return value, so a return-value encoding failure cannot override the business error.

When a method succeeds but its return value cannot be encoded, the caller gets `gor.reply_encode_failed`. No return value is available. If not even the call result came back, the caller sees a delivery failure; that equally does not prove the method did not execute.

## This version's boundary

This version does not register arbitrary error types, does not restore error fields, does not preserve error chains or merge structure, and adds no codegen annotation for error codes. Applications that need business data across nodes should put it in normal return values or persistent state, not smuggle it through error objects.

## Migration

Application code that branches on sentinels should replace the sentinel with a declared `gor.Code` and keep using `errors.Is`. For example, the device-shadow HTTP handler's HTTP 400 check should test `shadow.workshop_id_required` instead of an error object only recognizable in-process.

Simulators and tests must classify only by stable codes or the caller's own cancellation errors. Errors without a determinate code must be reported as unclassified, not categorized by guessing at text.

## Gap

Stable codes, cross-node `errors.Is` parity, reply-encoding priority, and the shadow and simulator migrations are implemented. Still not provided: arbitrary error type recovery, error field or chain fidelity, `errors.Join` structure fidelity, cancellation-frame or remote-deadline propagation. These are explicitly out of scope for this version.
