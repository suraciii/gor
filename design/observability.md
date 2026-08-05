# Observability

## Conclusion

`gor` is responsible for providing a minimal set of runtime observability facts.

Application-side proxies can count calls but cannot see the activation directory or mailboxes. They cannot reliably answer how many activations exist, nor find entities with a backlog. Only the runtime holds these facts.

`gor` is not responsible for aggregation, storage, export, or alerting. Those depend on the monitoring system the application already has. Building them into the library would pull in dependencies and decide labels, retention, and sampling policy for the user.

## The exposed facts

Only two kinds of facts are exposed.

### Activation snapshot

```go
type Activation struct {
	Identity Identity
	Queued   int
}

func (rt *Runtime) Activations() []Activation
```

`Activations` returns the activations on this node in the `active` state. Instances being created, deactivating, or already stopped are not in the result.

The result is sorted by `(Identity.Type, Identity.Key)`. One call returns a copy of one point in time. It is not retained and does not refresh itself.

`len(rt.Activations())` answers how many activations exist right now. `Queued` answers whose mailbox is backing up. Comparing it with the runtime's configured capacity tells how far from overload rejection you are.

`Queued` does not include the method currently executing. It only counts calls that entered the mailbox and have not started.

The snapshot observes only this node. In cluster mode, each node collects on its own; cross-node aggregation belongs to the application's monitoring system.

Activation time, last-used time, the executing method, and per-entity cumulative counts are not exposed. These values would let the runtime make no new decision, yet they grow state, lock contention, and label cardinality.

### Call completion

```go
type CallObservation struct {
	EntityType string
	Method     string
	Duration   time.Duration
	Err        error
}

func OnCall(func(CallObservation)) Option
```

`OnCall` follows the configuration shape of `OnError`. It is a callback, not an exporter interface. There is exactly one action here; inventing a single-method interface for it has no value.

A call made through an entity proxy or `Runtime.Invoke` fires the callback once, after the outcome is settled and before returning to the caller. `Duration` spans from entering the runtime to the settled outcome, including routing, activation, queuing, and method execution, not the callback itself. `Err` is the same error this call returns to the caller.

Applications choose metric dimensions with `EntityType` and `Method`, record latency distributions with `Duration`, and compute error rates with `Err != nil`. The entity key is not in the event. Using unbounded keys as metric labels lets the monitoring system run away; investigating a single entity's backlog should use the activation snapshot.

After caller cancellation, the callback still fires, with `Err` being the cancellation error. A canceled method may keep executing, but no second completion event is emitted. The event describes the outcome the caller saw; it does not pretend to know the final business outcome.

An entity method delivered by a scheduled task is an ordinary call and produces this event. `OnDeactivate` is not a call and produces none; its failures still go only through `OnError`.

When a forwarded call completes, the originating node records one end-to-end call. The receiving node must not record the same logical call again. Inbound forwarded calls still go through the same local execution path; no second dispatch semantics are set up.

The callback runs synchronously in the call's goroutine, after the outcome is settled. The runtime sets up no queue, goroutine, or clock for it. A callback must not block or do I/O; otherwise it is the caller itself that gets delayed. Callback panics are handled like `OnError` panics: as ordinary user callbacks; the runtime does not recover.

No built-in counters, histograms, traces, or metrics exporters. The callback already hands the application the facts needed to compute error rates and latency quantiles; keeping an aggregate state in addition would only add hot-path work and make reset, label, and export semantics the library's responsibility.

## Time and determinism

With `OnCall` enabled, the runtime reads start and end times from the injected `Clock`; `Duration` is their difference. Production paths must not call `time.Now()` directly.

The snapshot carries no timestamp and reads no clock.

Callbacks have no async delivery. They add no goroutine, introduce no new waiting, and create no timers. In `synctest`, callback order is therefore the call-completion order; with a fake Clock, durations are also deterministic results of fake time.

The snapshot copies active activations only inside the existing directory lock and reads each mailbox's current length. It waits on no channel, calls no user code while holding the lock, and modifies no runtime state.

User callbacks are still user code. Callbacks in simulation tests must also obey the project's channel and injected-Clock constraints.

## Hot-path cost

With `OnCall` off: no extra Clock read, no observation value constructed, no allocation, no locking, no goroutine. The call path keeps exactly one nil check.

This is not zero cost in the machine-instruction sense. A runtime-configurable feature cannot also be branch-free. The promise here is zero measurement work; that one check must stay at the outermost layer and must not spread into activation, mailbox, or dispatch paths.

Enabled, each call adds two `Clock.Now` reads, one stack-allocated `CallObservation` value, and one direct callback. The callback's own cost is the application's to bear.

The existing invocation round-trip baseline is 0.89 us/op. The implementation must record both the disabled and enabled-empty-callback results under the same conditions: disabled stays at zero allocations, and sustained regression against the baseline must stay within 5%; the in-library overhead of an enabled empty callback must stay within 20%. These numbers are not CI gates, but any exceedance must prompt a re-examination of the design, not silent acceptance.

`Activations` is an operations query, off the call hot path. Its time and allocation scale with the number of activations on this node.

## Testing

Unit tests verify the following facts:

- A call queued behind a blocking call appears in the corresponding activation's `Queued`.
- The snapshot excludes creating, deactivating, and stopped activations, and the result is stably sorted.
- Success, overload, activation failure, method error, and caller cancellation each produce exactly one completion event; when a canceled method completes later, no second event is produced.
- An injected fake Clock yields exact `Duration` values. The observation implementation adds no wall-clock reads.
- With the callback disabled, no events, Clock reads, allocations, or goroutines are added.
- The callback runs synchronously after the call outcome is settled; tests collect callbacks through a buffered channel, with no sleep or polling.

Once forwarding is implemented, simulation tests must also verify that one logical call is recorded once, at the originating node, and that observation events introduce no new decision sequence under a fixed seed.

The performance test reuses the invocation round-trip benchmark with an added variant carrying an empty `OnCall` callback. Both report `ns/op` and `allocs/op` and are compared with the baseline against this document's cost bounds.
