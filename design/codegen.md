# Code generation

## What this solves

Go generics cannot synthesize a type implementing `T` from `T` itself. So

```go
acct := gor.Ref[Account](rt, "alice")   // returns Account
```

cannot be done with generics alone. The common solution in similar Go projects is to drop the types:

```go
resp, err := system.AskGrain(ctx, identity, msg, timeout)   // any in, any out
```

goakt does exactly this (measured: `AskGrain(ctx, *GrainIdentity, message any, timeout) (any, error)`, `GrainContext.Message() any` / `Response(any)`). The cost is that all type errors are deferred to runtime.

`gor` does not accept that cost and goes with code generation.

## The input contract

The generator reads user-written Go interfaces. A valid entity interface method must:

- take `context.Context` as its first parameter
- return `error` last
- have encodable parameters and return values in between

The number of return values is unrestricted. "Encodable" means `encoding/json` can encode it: local calls pass values through without serialization, but the same method goes through JSON when forwarded, and an unencodable type blows up at exactly that moment, when it is already too late.

```go
type Account interface {
    Deposit(ctx context.Context, amount int64) (int64, error)
}
```

A method that does not satisfy the contract makes the generator report an error with the line number — **no silent skipping**. Silent skipping lets users believe the method was generated and only discover otherwise at runtime.

This contract comes from `alecthomas/go-rpcgen`'s approach (interface + named return values + trailing error), a shape proven in Go.

## Which interfaces are generated

Only marked ones are generated:

```go
//gor:entity
type Account interface { ... }
```

The generator ignores the package's other interfaces.

The "all exported interfaces in the package" rule is rejected: then an ordinary helper interface would also be checked against the contract, and users would have to move it to another package to silence the generator. The marker is explicit and coexists with "report errors on contract violations": unmarked ones are not checked; marked ones must comply.

## Output

Each interface gets one generated proxy:

```go
type accountProxy struct {
    id gor.Identity
    rt gor.Invoker
}

func (p *accountProxy) Deposit(ctx context.Context, amount int64) (int64, error) {
    var reply accountDepositReply
    err := p.rt.Invoke(ctx, p.id, "Deposit", &accountDepositRequest{A0: amount}, &reply)
    return reply.R0, err
}
```

Plus a server-side dispatch function that turns a method name plus arguments back into a direct call on the implementation type.

## Arguments and return values each go into a struct

`Invoke` takes one value on each side, while a method can take several parameters and return several values. So **each method generates a pair of structs**, used by both the proxy and the dispatch function:

```go
type accountDepositRequest struct { A0 int64 }
type accountDepositReply   struct { R0 int64 }
```

No special case for "only one". Passing `&amount` directly for a single value does save one layer, but then the generator has two paths, and two paths each need their own tests and maintenance. Nobody reads generated code; one less layer of indentation is not worth that price.

**Methods with no parameters or only an `error` return still get the empty structs.** Across nodes, this pair of structs is the bytes on the wire: empty structs encode to `{}`, and there is exactly one encode/decode path; without them, an "is it nil" check would be needed before and after encoding. Without a network it is indeed dead code; with a network it is the shortest form of that path.

## How generated artifacts plug into the runtime

The artifacts land in a subpackage, and the user's interface package does not import it (reason: the type-checking deadlock below). So how does the runtime know where `Account`'s dispatch function lives?

The generator additionally emits an install function:

```go
gorgen.Install(rt)
```

It registers each interface's dispatch function and proxy constructor on `rt`. After that:

```go
gor.Register[Account](rt, factory)        // dispatch function taken from the registry
acct := gor.Ref[Account](rt, "alice")     // proxy taken from the registry; return type is Account
```

`Register` therefore loses one parameter: the hand-written `dispatch` from [step 1](../ROADMAP.md#1-single-process-runtime) is taken over by the generator. This is a planned breaking change.

**Automatic registration via `init()` is rejected.** It saves the `Install(rt)` line, at the cost of users having to remember a blank import — forget it and the failure only shows up at runtime — and the registry becoming a process-wide global, while step 4's simulation tests run several nodes in one process. With the registry on `rt` and `Install` called explicitly, both problems disappear together.

The generator does not hardcode type-name strings. It emits a generic call like `gor.InstallType[Account](rt, dispatchAccount, newAccountProxy, newAccountCall)`, and `gor` itself computes the names with the same rules as `Register` / `Ref`: one shared function for all three places, so they cannot disagree.

## How the server rebuilds types from bytes

A forwarded call carries only a method name and a JSON blob (envelope in [cluster.md](cluster.md)). `gor` does not know what `"Deposit"`'s arguments decode into; only generated code does. So each type gets one more function:

```go
func newAccountCall(method string) (args any, reply any)
```

It builds a pair of empty shells by method name: `"Deposit"` yields `&accountDepositRequest{}` and `&accountDepositReply{}`. An unrecognized method name yields nil for both — something that really happens between nodes on mismatched versions.

**The name carries the type, like `dispatchAccount` and `newAccountProxy`.** A package can hold several entity interfaces; a `newCall` without the type name would not compile once there is a second one. Everything in the artifacts that is generated per type carries the type in its name; no exceptions.

From here on it is all existing machinery: `json.Unmarshal` fills the args, they go through **the same `Invoke`**, and the result comes back as `json.Marshal(reply)`. From this point, forwarded calls and calls initiated by local proxies share one path; serialization, activation, and dispatch are not duplicated.

**Only `newCall` exists for the network.** Proxies, dispatch functions, and structs existed anyway. This is also why `Invoke`'s argument changed from `[]any` to `any`: `[]any` cannot hold the types back, a struct pointer can.

`Register` returns an error for a type missing from the registry. `Ref` has no error return; it panics when the type is missing. This is not a runtime condition; it is wiring that was never connected: the same type's `Register` reports first at startup, and the `Ref` panic is only a fallback.

## The key decoupling

Generated artifacts depend on one narrow interface:

```go
type Invoker interface {
    Invoke(ctx context.Context, id Identity, method string, args any, reply any) error
}
```

This one line decides the stability of generated code: **no matter how the runtime changes internally, nothing is regenerated.** New transport, new directory, new encoding — the artifacts do not move.

This trick comes from `segmentio/glue`: a narrow `Call` interface that fully decouples generated artifacts from transport implementations.

## Implementation path

Load packages with `golang.org/x/tools/go/packages`, get type information from `go/types`, emit code with `text/template`.

**A known pitfall**: `go/types` requires the loaded package to pass type checking. If the artifacts lived in the same package as the user interface, then "the artifacts do not exist yet" → "user code references them → the package fails type checking" → "the generator cannot load the package" — a deadlock.

The solution: **the artifacts land in a subpackage** (for example `internal/gorgen/`). The package holding the user interface does not import the artifacts; `gor.Register` / `gor.Ref` connect them at runtime through the registry.

## How the generator is tested

`go/packages` spawns a `go list` subprocess, hundreds of milliseconds each time. The default test suite's rules are under 50 ms per test and no subprocesses, so the generator splits into two layers:

- **Loading layer**: `go/packages` + `go/types` → a pure-data model (interface name, method names, parameter type strings, return type strings, line numbers on error).
- **Rendering layer**: model → source code.

The rendering layer does not know `go/types`; tests hand-build the model directly, run fast, and live in the default suite. The loading layer's end-to-end tests really start the toolchain and get their own target.

The split is not just for testing. Contract violations are reported in the loading layer, and the model the rendering layer receives is compliant by construction — the two layers' responsibilities were always separate.

## Invocation

```bash
go run github.com/suraciii/gor/cmd/gorgen -pkg ./domain
```

Or `//go:generate`.

No automatic generation on `go build`: Go has no such hook; forcing it would need a wrapper script, and that would make the most common command, `go test ./...`, behave unpredictably. Generation is an explicit step, and CI gets a "generated artifacts match the source" check.

## Gap

**`newCall` is already produced by the generator, and `Invoke`'s argument is already `any`.** These artifact changes were completed when 6b forwarding was connected; the artifacts now serve local and forwarded calls alike.

**Import alias collisions.** The generator writes imports by package name; two packages with the same name at different paths (`a/domain` and `b/domain`) produce code that does not compile. This only happens when a method signature uses cross-package types. The generator should assign its own aliases; it does not yet.

## Rejected approaches

**Runtime reflect-synthesized proxies.** Go's `reflect.MakeFunc` can construct function values but not a type implementing an arbitrary interface. It cannot be done.

**The other way around: users write a struct, the generator produces the interface.** One less hand-written piece, but users would not see their own API surface — and the API surface is the most important thing callers see.

**protobuf/IDL first.** It would require users to learn another language on top of Go, and IDL cannot express the whole Go type system. gor's stance: the Go interface is the IDL.
