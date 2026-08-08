# Vision

## In one sentence

`gor` is a Go port of the Orleans runtime model for programs that need
stateful Grains on one machine.

## The product direction

`gor` gives a Go program a Grain Runtime. A Grain has a GrainType, a
GrainKey, State, and behavior. The runtime starts a Grain when a Call needs
it. It keeps Calls for one Grain in order and keeps confirmed State in local
storage.

The first product is one Silo on one machine. It must be useful without a
network, a sidecar, or a remote service. A later cluster extension may move
Grain ownership between Silos. It must not change the Grain model.

## Core promises

### Orleans model

`gor` uses the Orleans model and terms. A Grain Reference names a Grain
without starting its Activation. The first Call can start the Activation.
The Grain may leave memory later. Its GrainId and confirmed State remain.

The Go API may use Go forms where the languages differ. The runtime meaning
must stay aligned with Orleans unless a product spec states a difference.

### Reliable single Silo

The single-Silo product must make these results dependable:

- Calls for one Grain run one at a time.
- State survives a normal process restart.
- State writes report conflicts instead of silently replacing newer State.
- Reminders survive a normal process restart.
- Lifecycle and background failures are visible to the Application.
- A hard failure does not silently report an unknown result as success.

The product must state loss and retry limits in user language. It must not
hide them in implementation details.

### Typed Calls

Applications define Grain interfaces in Go. Generated code gives callers a
typed Grain Reference. A wrong argument type must fail at compile time.

### Reproducible behavior

Tests must control time and I/O through explicit boundaries. Failure tests
must use a seed that can reproduce the same decisions. This rule applies to
the single-Silo product and to the future cluster extension.

## Future cluster boundary

Cluster support is a later extension. It may add several Silos, shared Grain
ownership, routing, and transport. The 0.1.0 product does not promise these
features.

The public model must leave room for this extension. A Grain Reference must
not depend on a local memory address. State must keep a version that can
reject an old write. These are design constraints for future work, not
cluster promises in 0.1.0.

## Product boundaries

The 0.1.0 product does not provide:

- Call Filters.
- Reentrant or interleaved Grain Calls.
- Cluster operation or cluster administration tools.
- Rolling upgrades between incompatible Application versions.
- Unbounded scale.

These limits keep the first public release small and reliable. Later work
may add a capability only after its behavior and failure rules are defined.

## Related systems

`gor` is a library. It runs inside the Application and uses a local store.
It does not require a separate runtime service for the single-Silo product.

The project uses Orleans as its model reference. It does not promise source
or binary compatibility with Orleans. It promises the Orleans Grain model in
a Go API.
