# Example application

> Implemented. The runnable device-shadow service is in [../examples/shadow/](../examples/shadow/); this document explains the programming model and boundaries it demonstrates. The same service runs on one node or on several; the business code does not change between them.

## Why not a counter

A counter demonstrates gor's three concepts in ten lines. The problem: a counter does not show why you would use `gor` — a map plus a lock can do a counter too.

The example app answers a different question: when would you be glad you used this?

## The story

**Device shadow.** A large fleet of devices out in the field; each reports state periodically; the server must answer "what is this device like right now" at any time, and mark a device offline when it goes quiet.

Chosen because it stresses four things at once — exactly the four reasons `gor` exists:

**Large count, mostly idle.** A hundred thousand devices, but only a few hundred speaking at any moment. The traditional approach keeps all hundred thousand objects in memory, or reads the database on every report. `gor`'s answer: the device itself is an entity — in memory while speaking, gone when silent, with state left in the store.

**Concurrent writes to one device must be serialized.** The device reports state while operations pushes configuration. When the two collide, without serialization you write a pile of optimistic-lock retries. In `gor` this is the default; users do nothing.

**Offline detection is naturally a scheduled task.** "No report for thirty seconds means offline" — this needs an alarm that follows the device and survives process restarts. That is exactly what the scheduled-task step provides. Polling the whole table for the same job gets linearly more expensive with device count.

**Aggregation needs cross-entity calls.** "How many devices in this workshop are online" requires devices to reach the workshop. This demonstrates entity-to-entity calls, and why the reverse cannot work — the workshop cannot hold references to a hundred thousand devices and ask them one by one.

## What the reader should learn

In this order, someone new to `gor` should be able to:

1. Recognize what should be an entity — what an identity is, where the boundaries go.
2. Know how state is stored, when it is persisted, and what a write conflict does.
3. Attach a scheduled task and know it survives a process restart.
4. Know what state the world is in after a call fails — the point examples most easily gloss over.

Point 4 must be written seriously. **No ignored errors in the example.** Every failure is either handled, or a comment explains why it can be ignored here. Examples are copied; copying a `_ = err` copies a bug.

## Boundaries

**The example must not modify the library.** When writing it surfaces an awkward API, a missing capability, or a doc that cannot say what it means — that is the example's most valuable output: record it. Do not work around it in the example, and do not change the library to accommodate it.

**No web framework, ORM, or config library.** The standard library is enough. The example should teach `gor` usage, not someone else's.

**No frontend.** An HTTP interface you can hit with `curl` is enough.
