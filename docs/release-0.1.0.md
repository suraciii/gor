# 0.1.0 Product Contract

This document states what gor 0.1.0 promises. It is not a status report.
[ROADMAP.md](../ROADMAP.md) states what is done. The [design
document](../design/release-0.1.0.md) states how the release is delivered.

## Release promise

gor 0.1.0 is a reliable single-Silo Grain Runtime for Go Applications. It
starts Grains when Calls need them, serializes Calls for each Grain, and
keeps confirmed State in local storage.

The release targets one process and one local store. It does not require a
network or a cluster.

The release is ready only when users can understand the rules, test failure
cases, and use the public API without private runtime code.

## Supported capabilities

### Grains and Grain References

A Grain is identified by a GrainType and a GrainKey. Together they form a
GrainId.

A Grain Reference names a Grain without starting it. The first Call can
start its Activation. An idle Grain can leave memory. A later Call can start
it again and load its State.

Calls for one Grain run one at a time. Calls for different Grains may run at
the same time. The Runtime defines results for overload, timeout, cancel,
start failure, method failure, panic, and shutdown.

### Calls

The public API uses typed Go interfaces. A caller uses a typed method call.
It does not send an untyped message.

A Grain may call another Grain through a Grain Reference. Local and future
remote Calls use the same model.

The Runtime does not re-enter a Grain during a Call. Reentrant and
interleaved Calls are outside 0.1.0. A call cycle fails instead of waiting
forever.

The Runtime does not retry a Business Action after an unknown result. The
Application must decide whether a Safe Repeat is valid.

### Request Context

A Call may carry Request Context, such as a trace ID. The called Grain can
read it during the Call. The Grain Runtime does not save it.

### Persistent State

A Grain can own named State values. A successful State write becomes the
confirmed value for the Grain.

State provides these user-visible operations:

- Read the current value.
- Write a new value.
- Check whether a value exists.
- Clear the value.

An absent value is different from a value that contains the type's zero
value. Clear removes the confirmed value. A later Activation observes that
the value is absent.

A version check stops an old Activation from replacing newer State. The
Runtime reports a conflict.

State has two durability levels:

- **Full**: confirmed writes are on disk before the Call returns.
- **Relaxed**: a hard machine failure may lose recent confirmed writes.
  A normal process restart keeps confirmed writes.

State is Application data. The Application owns its meaning and its format
changes.

### Reminders

A Grain can set a named Reminder. A Reminder can run once or repeat on a
period. The setting, a new setting, and cancellation survive a normal
process restart.

A due Reminder can start a Grain that is not in memory. The Runtime claims a
Reminder before it delivers the Call. A failure after the claim can miss
that delivery. The Runtime does not retry the Call automatically.

A periodic Reminder reports its first tick time, period, and current tick
time to the Grain. A late process does not receive every missed tick.

An Application that needs recovery must save a pending Business Action and
use a Safe Repeat handler.

### Lifecycle and background errors

A Grain can run code when its Activation starts and when it leaves memory.
The leave reason tells the Application why the Activation ended.

Failures with no waiting caller go to the configured background error sink.
Examples include a failed Reminder Call and a failed deactivation hook. The
sink reports the original error and its source. It does not add hidden
retries.

### Observability

The Runtime provides a snapshot of this Silo's active Activations and a
completion event for each Call. The Application chooses its own metrics,
traces, storage, and alerts.

## Single-Silo boundary

The 0.1.0 release is a single-Silo product. The single-Silo path must not
need a network, a remote service, or cluster membership.

GrainId, Grain Reference, Call, State, Reminder, and encoding boundaries
must leave room for future cluster work. This is a design rule. It is not a
promise of reliable cluster operation in 0.1.0.

Multi-Silo operation, ownership changes, network failure handling, rolling
upgrades, and cluster administration are outside the 0.1.0 promise.

## Non-goals

0.1.0 does not provide Call Filters, reentrant or interleaved Grain Calls,
cluster operation tools, incompatible rolling upgrades, cloud storage, or
unlimited scale.

These limits keep the single-Silo product small and reliable.

## Acceptance standard

The release is ready only when all items below are true:

1. A small example can define a Grain, get a typed Grain Reference, write
   and clear State, and set a Reminder through the public API.
2. The example can stop and start the process without private runtime calls
   or manual database repair.
3. Deterministic tests cover activation, serialized Calls, State conflicts,
   State clearing, process failure, Reminder claims, Safe Repeats, cancel,
   shutdown, and background errors.
4. The docs state the result for timeout, cancel, delivery failure, and a
   claimed Reminder whose Call did not run.
5. The full repository test gate passes. It includes unit tests, simulation,
   generated-code tests, network tests, lint, and race tests.

## Gap

The conformance Application in `examples/shadow` now composes the
single-Silo Runtime, State, Reminders, typed Calls, Request Context, lifecycle,
and observations under restart and failure. It keeps business records in a
separate ApplicationStore and uses ActionID Safe Repeat. The remaining release
status is tracked in ROADMAP.md and the release gates.
