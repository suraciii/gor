# Orleans Grain Runtime

This context defines the Orleans language used by gor. gor is a Go port of the
Orleans runtime model. Use these terms in product docs, design docs, examples,
and new code names. Keep one term for one concept.

## Grain model

**Grain**:
A stateful object identified by a GrainId. A Grain has state and behavior.
_Avoid_: entity, actor, service, worker

**GrainId**:
The stable identity of one Grain. It contains a GrainType and a GrainKey.
_Avoid_: identity, address, instance ID, object ID

**GrainType**:
The kind of Grain. Grains with different GrainTypes do not share a GrainId.
_Avoid_: class, model, category

**GrainKey**:
The application value that selects one Grain of a GrainType.
_Avoid_: ID, name, identifier

**Grain Reference**:
A typed value that names a Grain without creating it.
_Avoid_: proxy, handle, stub, pointer

**Call**:
A request to run one method on a Grain through a Grain Reference.
_Avoid_: message, invocation, packet

**Request Context**:
Small data attached to a Call. The called Grain can read it during the Call.
Request Context is not State and the Grain Runtime does not save it.
_Avoid_: header, context value, request property

**Call Filter**:
Shared policy that runs before or after a Call.
_Avoid_: interceptor, middleware

## Runtime model

**Grain Runtime**:
The part of the application that starts Grains, accepts Calls, keeps State,
and runs Reminders.
_Avoid_: engine, server, actor system, framework

**Activation**:
The live form of a Grain that can receive Calls. A Grain can lose its
Activation without losing its GrainId or State.
_Avoid_: instance, process, actor

**Deactivation**:
The end of an Activation. Deactivation does not delete a Grain's GrainId or
State.
_Avoid_: destroy, delete, terminate

**Lifecycle**:
The path from Activation to Deactivation for one Grain.
_Avoid_: object lifetime, process lifetime

## State and reminders

**State**:
The current data owned by a Grain. State describes the Grain now.
_Avoid_: status, condition

**Confirmed State**:
State that the Grain Runtime has accepted as the current value for a Grain.
_Avoid_: saved state, cached state, best-effort state

**Durability**:
The amount of Confirmed State that remains after a machine failure.
_Avoid_: speed mode, flush mode

**Reminder**:
A future Call that the Grain Runtime remembers for a Grain. A Reminder can
happen once or repeat on a period.
_Avoid_: timer, wake-up, scheduled task, job

## Call results

**Conflict**:
A result that says a write used old State. A newer State already exists.
_Avoid_: collision, race error, stale write error

**Unknown Result**:
A result where the caller cannot know if a Business Action happened. A timeout
or delivery failure can cause an Unknown Result.
_Avoid_: failed call, lost call, partial success

**Business Action**:
A change that the application asks a Grain to make.
_Avoid_: side effect, operation, command

**Safe Repeat**:
A Business Action that does not apply the same business change twice when it
runs more than once.
_Avoid_: idempotent action, exactly-once action

## Boundaries

**Application**:
The program that defines Grains and their business rules.
_Avoid_: client, consumer, user code

**Silo**:
A process that hosts a Grain Runtime and its Activations. In 0.1.0, one Silo
runs on one machine.
_Avoid_: node, machine, server, worker

**Single Silo**:
One Silo with local State. This is the main gor product in 0.1.0.
_Avoid_: standalone mode, local cluster

**Cluster**:
Several Silos that share Grain ownership. Cluster support is an optional
extension.
_Avoid_: multi-node runtime, shared service

**Ownership**:
The Silo responsible for serving a Grain in a Cluster.
_Avoid_: placement, assignment, shard owner
