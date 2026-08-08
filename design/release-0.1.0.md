# 0.1.0 Delivery Design

This document lists the work for the [0.1.0 product
contract](../docs/release-0.1.0.md). The other design documents define local
APIs and algorithms. This document defines the work order and the proof that
the parts work together.

## Design rules

The main product is one process with one local store.

All input and output uses an interface. All time comes from the injected
`Clock`. A channel, not a mutex, waits for another call. Each component is an
explicit state machine. These rules make failure tests repeatable.

The application owns its local business records. The Runtime owns Grain State
and Reminder records. The Runtime does not combine application records with
these runtime records.

Future cluster work needs clear GrainId, Grain Reference, Call, State, and
encoding boundaries. Cluster health and cluster operations are not 0.1.0
gates.

## Work packages

### 1. Freeze the contract

Write one table for these rules:

- GrainId, GrainType, and GrainKey names;
- Call order, overload, timeout, cancel, panic, and the non-reentrant rule;
- start and deactivation transitions;
- State read, write, Exists, Clear, conflict, unknown store result, and restart;
- Reminder setting, claim, cancel, tick status, and method failure;
- background error sources and call observations;
- application data and safe repeat rules.

The table must link to the detailed documents. It must not copy API text.
Behavior not in the table is not a release promise.

### 2. Harden the Grain runtime

Use one call path for local Calls and future remote Calls. The root Runtime
must decide if it still accepts a Call before the Call reaches a Grain.

For each GrainId, start, queue, method run, and leave must follow one state
machine.

Tests must cover:

- two callers that start one key at the same time;
- a method panic with calls in the queue;
- cancel while a call waits and while a method runs;
- start failure and state write failure;
- normal close and forced stop;
- leave reason and background error reporting;
- a type name that does not depend on an incidental Go type string.

The runtime does not retry a business method. The caller must decide if a
retry is safe.

### 3. Harden state and Reminders

Keep State and Reminders as separate interfaces. Keep the version check on
State writes. A Reminder claim must select one delivery attempt. `Exists` and
`Clear` must distinguish absent State from a present zero value.

Tests and simulation must cover:

- due work found after a process restart;
- two pollers that claim one record at the same time;
- a failed claim;
- process failure after claim and before method entry;
- a failed Reminder method;
- cancel and reset;
- a periodic Reminder after downtime without replaying every missed time.
- State that is absent, present with a zero value, and cleared;
- first tick time, period, and current tick time for a periodic Reminder.

The result is at-most-once delivery. Recovery is an application pattern:
save a pending action, wake a fixed Grain, and make the handler safe to run
more than once. The runtime gives the Reminder and call paths. It does not own
the business record.

### 4. Complete call data and encoding

Request Context is data that travels with a Call, such as a trace ID. Add the
smallest call path that supports it:

- the caller can add Request Context;
- the method can read incoming Request Context;
- local Calls and future remote Calls use the same call path;
- no shared Call Filter pipeline is part of 0.1.0;
- the call path cannot bypass call admission or change call order;
- stable error codes cross the call boundary;
- cancel rules stay clear after a method was sent.

Keep JSON as the current encoding. Do not add an encoding plug-in or a
zero-downtime upgrade format without a real user need. Typed interfaces,
stable type names, and written compatibility rules define the application
contract.

The Request Context API, lifetime, encoding, and failure rules are in
[request-context.md](request-context.md).

### 5. Build the integration sample

Add a small example and tests that use only public APIs. It must contain:

- a Grain with current State;
- a fixed-key coordinator or dispatcher;
- a saved pending action;
- application records in its own local rows;
- a periodic recovery Reminder;
- a handler that is safe to run more than once;
- a process stop between save and delivery;
- a repeated delivery that does not apply the business change twice.

The example is not a second framework. It proves that the public Runtime
boundaries support a real durable application pattern with local application
data and safe repeat handling. The required Single Silo flow and its failure
evidence are specified in [conformance-example.md](conformance-example.md).

### 6. Run release checks

The release candidate must pass:

- `make test`;
- `make sim`;
- `make gen`;
- `make net`;
- `make lint`;
- `make ci`;
- race tests;
- a clean-module example build and run.

Storage tests that measure disk durability must use a real-disk path. The
simulation must report nonzero test cases and repeat the full seed set with
the same result.

## Failure table

| Failure | Runtime result | What the application must do |
| --- | --- | --- |
| Queue full | The call is rejected. The method does not start. | Apply back pressure or retry when safe. |
| Caller timeout or cancel | The caller stops waiting. The method may continue. | Use a safe repeat rule or a repair action before retry. |
| Method panic | The call fails. The Grain instance is removed. | Fix the method and decide if a retry is safe. |
| State conflict | The write fails. The old instance is not trusted. | Read again and use a clear retry rule. |
| Store result is unknown | The call fails. The write may or may not exist. | Check the stored result before a non-repeatable action. |
| Claim succeeds, then process stops | The Reminder may be missed. | Save a pending action and recover it safely. |
| Reminder method fails | The error goes to the background sink. No hidden retry starts. | Save retry state in the application when needed. |
| Normal close | New calls are rejected. Accepted calls follow close rules. | Stop new outside work and drain as needed. |
| Forced stop | Queued calls are rejected. A running method may finish later. | Recover work whose result is unknown. |

The table must state unknown results. The runtime must not turn an unknown
result into a false success or an unsafe retry.

## API usability gate

The first example is the main usability test. A user must be able to:

1. declare a typed Grain interface;
2. create named State in a factory;
3. get a Grain Reference by type and key;
4. call another Grain with the Binder;
5. add Request Context and read it in the called Grain;
6. set and clear a Reminder;
7. set an error sink and Call observation;
8. close and reopen the Runtime.

Each step must have one clear public path. The example must not need cache
details, private store layout, cluster membership, or a second hidden retry
loop. The clean consumer build and two-process run are additional evidence
for the conformance example.

## Work order

Work lands in reviewable batches. Stop after each batch:

1. contract and acceptance table;
2. Grain runtime edge rules;
3. state and Reminder failure rules;
4. call data and encoding boundaries;
5. integration sample and failure tests;
6. release docs and clean-install check.

The first batch is documentation only. Code work starts after review of the
contract.

## Gap

The current design documents cover most single-node parts. This document adds
one failure table, one integration sample, and proof that the parts work
together after restart, failure, and a safe repeat.
