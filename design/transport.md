# Transport

Moves bytes between nodes. **This layer does not understand message semantics** — it does not know what an Identity, a method, or an entity is.

## No gRPC

The needed capability is exactly one: send bytes to the other side and get bytes back.

The protobuf contracts, HTTP/2 stack, and connection-management policies that come with gRPC are things to work around or fight. And DST requires the transport to sit entirely behind an interface, replaceable by an in-memory fake — a thin layer of our own is easier to build and easier to understand.

## Interface

```go
type Handler func(ctx context.Context, payload []byte) ([]byte, error)

type Transport interface {
    Send(ctx context.Context, addr string, payload []byte) ([]byte, error)
    Serve(ctx context.Context, h Handler) error
    Addr() string
    Close() error
}
```

An address is a string, not a node type from `cluster`. The transport does not import `cluster` — the dependency runs the other way.

**Binding is separated from serving.** The listen address is bound at construction, so `Addr()` can immediately return the actually bound address, and `Serve` is just the accept loop. A node must know its own address before writing its row into the membership table, and tests use `:0` to let the kernel pick a port — without the separation, this is impossible.

## One connection per direction

A request from A to B goes over the connection A dialed to B, and B's reply comes back over the same one. A request from B to A goes over the other connection, dialed by B.

Two nodes therefore have two TCP connections, not one. The cost is explicit; in exchange we drop arbitration logic like "can an inbound connection be used for outbound requests" — logic that only runs in the race where both sides dial at the same time, the classic "write ten lines, regret them for three years".

## Frames

```
[4-byte payload length][8-byte correlation id][1-byte type][payload]
```

Big-endian. Three types: request, response, error.

**Lengths have an upper bound; exceeding it drops the connection.** This is not defensive special-casing — without a bound, a wrong length prefix makes the peer allocate gigabytes. The bound is a constant, not a configuration knob.

**Errors carry text only, not types.** The error frame's payload is just the error string. Whoever needs structured errors encodes them into a normal response payload and decodes them in the layer above. A transport that recognizes error types would be recognizing semantics.

## Multiplexing

Multiple requests fly on one connection at the same time, matched by correlation id.

**Only one goroutine touches the pending table.** Each connection has three goroutines: reader, writer, owner. The owner holds the pending table and the next correlation id; its inputs are all channels — new requests, response frames read, canceled requests, a dead connection.

No mutex protects the pending table. This is not cleanliness for its own sake: blocking on a mutex is not durably blocking in `synctest`, and one mutex can keep the bubble from detecting quiescence. The same reason already appeared once with the activation placeholder in `runtime`; this is the second time.

`Send` hands the request together with a reply channel to the owner, then selects on the reply and `ctx.Done()`.

**Each server-side handler runs in its own goroutine; it must not run in the owner.** The owner only registers and hands over; running a handler to completion in the owner would block every other request on this connection behind it — the entire reason correlation ids exist is so requests do not wait on each other. The layer above can least afford this: `gor` packs calls of many entities into one connection, and entity calls are serial anyway, so one busy entity would stall every other entity from the same node.

Handlers also write responses back to the owner through a channel; only the owner ever lays out frames. When the connection is dying, cancel the handlers' ctxs and wait for them to exit — leave one handler still running and `Close()` becomes a gamble.

## Unknown outcome

When `Send`'s ctx expires, the request may already have finished executing on the other side.

Cancellation does exactly one thing: tells the owner to drop this pending entry. A response that really arrives later finds no registration and is thrown away.

**An error the caller gets does not mean the other side did not execute.** This is the same semantics as timeouts in `runtime`, and the same class of thing as "write failed but took effect". The layer above must not treat a transport error as "it did not happen".

## A dead connection is dead

Lazy dialing: the connection is created only on the first send to an address.

When the connection errors, the owner returns all pending requests with the error and closes the connection. **No reconnect loop, no backoff, no keepalive probes.** The next `Send` finds no connection and dials again; if dialing fails, it reports the error.

Deciding whether a node is really gone is the membership table's job ([cluster.md](cluster.md)). The transport having an opinion here would only produce two contradictory judgments.

## Encoding is not this layer's business

The transport moves opaque bytes. Encoding happens in the `gor` layer with `encoding/json` — the same story as entity state persistence; no second serialization story is introduced.

It was chosen not because it is fast but because it is already in the project and humans can read it directly in production. [architecture.md](architecture.md) explains why no custom format, and what that gives up.

## Testing

Frames and multiplexing are all tested with `net.Pipe` — an in-memory bidirectional pipe, no network, in `make test`. Out-of-order responses, responses arriving late after a timeout, oversized frames, and mid-connection drops are all enumerated here.

Real TCP tests only the dialing and listening slice, on a separate `make net` target; like `make gen`, it stays out of the default tests.

The fault-injecting fake transport — partition, reorder, drop — belongs to the simulation tests; see [simulation.md](simulation.md). It implements the same `Transport` interface, not the same code.

## Gap

Under the `sim` build tag, `simulationTransport` already implements this interface and can drop messages by partition; the current implementation does not yet provide the delay and reorder injection listed in the design.
