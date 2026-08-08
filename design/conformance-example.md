# Public API conformance example

## Decision

Extend `examples/shadow` with a small recovery path. `Device` remains the
main Grain. Add a fixed-key `RecoveryCoordinator` Grain and an
Application-owned store for the recovery records.

The conformance path is Single Silo. It must not configure a `MemberStore`, a
Transport, or cluster options. The existing `net`-tagged cluster test is a
separate preview test. It is not part of this conformance path.

The example uses only public gor APIs for Runtime work. It does not read the
Runtime's private state, call a private runtime method, or repair a Runtime
database row.

## Grain shape

The example keeps the existing `Device` Grain and adds these conformance
methods to its typed interface. Existing device-shadow methods remain
available.

```go
type Device interface {
    Report(ctx context.Context, workshopID string, state string) error
    ReportAction(ctx context.Context, actionID string, state string) error
    Shadow(ctx context.Context) (Shadow, error)
    ShadowExists(ctx context.Context) (bool, error)
    ClearShadow(ctx context.Context) error
    ApplyPending(ctx context.Context, actionID string) error
}

type RecoveryCoordinator interface {
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    Recover(ctx context.Context, tick gor.TickStatus) error
}
```

The interfaces have the `//gor:grain` marker. The generated package supplies
the typed Grain References, dispatch functions, and Reminder call factory.
Startup calls the generated `Install` function before `Register` or `Ref`.

`RecoveryCoordinatorKey` is the only key used for the coordinator. Its value
is a fixed application constant, such as `"recovery"`. A device uses its
device key as its `GrainKey`. The coordinator calls each target through
`gor.Ref[Device](b, action.DeviceKey)`. This proves a typed cross-Grain Call
without adding cluster ownership to the example.

## Data ownership

The example must keep Runtime data and Application data in separate stores.

| Data | Owner | Boundary and contract |
| --- | --- | --- |
| Device shadow | Grain Runtime | A named `gor.State[Shadow]` called `"shadow"`. `Set`, `Exists`, `Get`, and `Clear` use the public State API. |
| Coordinator status | Grain Runtime | A named `gor.State[CoordinatorState]` called `"running"`. It is a restart-visible status value, not the recovery queue. |
| Recovery schedule | Grain Runtime | A `gor.Reminder[RecoveryCoordinator]` called `"recovery"`, set with `gor.Every` and `gor.Handle(RecoveryCoordinator.Recover)`. |
| Pending actions and applied records | Application | An application-defined `ApplicationStore` interface. Its SQLite implementation uses a separate `business.db` file and its own tables. |

The Runtime State store and Runtime Reminder store use the existing public
`store.Store` and `store.ReminderStore` interfaces. The ApplicationStore is
not a new gor feature. The Application owns its interface, schema, migration,
transaction, and close operation. Tests use an in-memory implementation with
fault injection.

The application store must not use a `store.GrainId` row as a disguised
business record. It must not inspect the Runtime's `records` or `schedule`
tables. A normal run opens two durable paths:

```text
runtime.db          Runtime Reminder and membership tables
runtime-state.db    Runtime Grain State created by store.OpenSQLite
business.db         Application pending actions and applied records
```

The example runs without membership, so the membership table is unused. The
ApplicationStore owns `business.db`; the Runtime owns the other two files.

## Application records

The ApplicationStore has the smallest interface needed by the two Grains:

```go
type ApplicationStore interface {
    SavePending(context.Context, PendingAction) error
    ListPending(context.Context) ([]PendingAction, error)
    ApplyPending(context.Context, string) error
    ReadApplied(context.Context, string) (AppliedRecord, bool, error)
    Close() error
}
```

`PendingAction` contains an application-generated `ActionID`, the target
device key, the reported value, and an optional trace ID. `AppliedRecord` is a
business receipt keyed by `ActionID`. The ActionID is the deduplication key;
the example never generates a new ActionID while retrying an uncertain
action.

`SavePending` inserts a new action. Repeating the same ActionID with the same
payload is a no-op. Reusing an ActionID with a different payload returns an
application error. `ListPending` returns only pending actions in a stable
ActionID order.

`ApplyPending` is one application transaction. It reads the action, creates
one applied receipt, and marks the action applied. A unique ActionID makes the
transaction safe to repeat. If the action is already applied, the operation
returns success without creating another receipt or changing the business
result.

The Runtime does not know these records and does not make this transaction
atomic with a State or Reminder write. The separate boundary is the reason
the ActionID and Safe Repeat rule are required.

## Call flow

### Start recovery

The application opens both stores, creates a Single Silo, installs generated
bindings, and registers both Grain factories. The factories capture the
ApplicationStore. They still receive the public `*gor.Binder` and create all
Runtime State and Reminder handles from that Binder.

The first call is:

```go
gor.Ref[RecoveryCoordinator](rt, RecoveryCoordinatorKey).Start(ctx)
```

`Start` sets the `"recovery"` Reminder to a short periodic interval and then
confirms the `"running"` State. If `"running"` is already present and true,
`Start` does nothing. This makes an optional startup check safe after a
restart; it does not reset the Reminder's first tick time.

`Stop` clears the `"running"` State first and then cancels the Reminder by
name. A successful cancel removes the persisted Reminder. If the process stops
after State clear and before cancel, a restart sees a stopped coordinator and
`Start` safely restores the Reminder. The example calls `Stop` only in the
cancellation test; recovery after a process stop does not call `Stop` or
rewrite the schedule.

### Save a pending action

The caller adds Request Context and calls one typed Device Reference:

```go
ctx, err := gor.WithRequestContext(context.Background(), "trace_id", "trace-1")
if err != nil {
    return err
}
err = gor.Ref[Device](rt, "device-1").ReportAction(ctx, "report-1", "temperature=20")
```

`Device.ReportAction` performs these steps in order:

1. Read `trace_id` with `gor.RequestContextValue` and validate it.
2. Save `PendingAction{ActionID: "report-1", ...}` through ApplicationStore.
3. Read the current shadow State.
4. Write the new shadow with `State.Set`.

The pending save comes before the State write so a deterministic
`ErrPendingActionConflict` leaves Device State unchanged. This is not a
distributed transaction: if a pending save succeeds but the later State write
has an uncertain result, retry the same ActionID. The copied trace ID is
application data. It is not Request Context after the write. Request Context
is not stored in Runtime State or Reminder records.
The `Recover` method must observe an empty Request Context because the
Runtime creates Reminder Calls with a fresh context.

`ShadowExists` calls `State.Exists`. `ClearShadow` calls `State.Clear`. These
methods make absence observable and prove that an absent State is different
from a present zero value.

### Recover after a stop

The deterministic restart test calls `rt.Kill()` after `Report` returns and
before the next Reminder delivery. It then creates a new Runtime with the
same public stores, installs the same generated bindings, and advances the
injected clock. No private runtime call and no database repair is allowed.

The persisted Reminder poller does this for each due row:

1. List the due Reminder.
2. Claim it with the public `ReminderStore.Claim` CAS.
3. Build `TickStatus` and invoke `RecoveryCoordinator.Recover` as an ordinary
   typed Grain Call.

`Recover` lists pending Application actions in ActionID order. For each one it
calls `Device.ApplyPending`. The Device delegates to the ApplicationStore
transaction. A successful recovery leaves one applied record and no pending
record for that ActionID.

The test also wraps the public `ReminderStore` in a test-only adapter that
blocks after a successful `Claim`. The test driver calls public `rt.Kill`
while the claim is blocked, then releases the adapter. This proves the
claim-before-delivery boundary: the claimed due time can be missed, the
Device method does not run, and the pending application record remains. A
periodic Reminder gives the Application a later recovery attempt. The test
does not insert or edit a Reminder row by hand.

### Safe Repeat

The Runtime promises at-most-once Reminder delivery for each claimed due
time. It does not promise exactly-once execution. A process stop after a claim
can lose that delivery. A caller timeout, cancellation, or unknown store
result can also leave the caller unable to know whether a Business Action ran.

The recovery method therefore uses this Safe Repeat rule:

> For one ActionID, `ApplyPending` may run any number of times, but the
> Application transaction may create one applied receipt only.

The repeat test invokes `ApplyPending` twice through the typed Device
Reference. A second call returns success and does not create a second receipt.
The stronger fault test commits the first application transaction and then
returns an injected unknown error. The error reaches the Reminder error sink;
the same ActionID is called again and the receipt count remains one.

The Runtime does not add a retry. The periodic Reminder and the Application's
ActionID rule provide recovery.

## Failures and Unknown Results

The example always installs both `gor.OnError` and `gor.OnCall`.

- A foreground Device or coordinator Call returns its error to the caller.
  `OnCall` records the method, duration, and error for the test.
- A failed `Recover` method has no waiting caller. `OnError` receives the
  original error with `ReminderInvocation{Method: "Recover"}`.
- A failed `OnDeactivate` hook is reported with `gor.Deactivation`.
- Reminder scan and claim failures are scheduler failures. They do not enter
  `OnError`; the test observes that no Device Call ran and that a failed claim
  leaves the row available. The next poll retries the scan.
- A failed Reminder method is not retried for the same due time. The
  application may recover the pending ActionID on a later periodic tick.

The example documents these intentional Unknown Results:

| Boundary | Unknown result | Required handling |
| --- | --- | --- |
| Device `State.Set` or `State.Clear` | A non-context store error can mean that the write committed or did not commit. The activation is discarded. | Call again to load confirmed State. Retry a Business Action only with the same ActionID and a Safe Repeat rule. |
| `ApplicationStore.SavePending` | The pending row can exist even when `ReportAction` returns an error. | Read by ActionID before creating another action. Retry the same ActionID. |
| `ApplicationStore.ApplyPending` | The application transaction can commit before its result reaches the Grain. | Retry the same ActionID. The unique receipt makes the retry a no-op. |
| `Reminder.Set` or `Reminder.Cancel` | The unconditional write or delete can be complete when the caller sees an error. | Repeat Set by the same name or repeat Cancel. Do not edit Runtime tables. |
| Call timeout or cancellation | The caller stopped waiting; the Grain method may have started and may have saved State or an action. | Treat the result as unknown. Query the application record and use the same ActionID before retry. |
| Process stop after Reminder claim | The due occurrence may be missed because claim happens before delivery. | Leave the pending action in ApplicationStore and wait for the next periodic Reminder. |

The Single Silo conformance path has no Transport, so it does not produce a
transport failure. The public call contract still treats a transport failure
as unknown when a clustered caller uses the API outside this example.

The example must not turn any row count, error, or timeout into a false
success. Every error from store open, Runtime creation, registration, a Call,
Reminder setup, ApplicationStore, or close is returned or reported. The only
errors not sent to `OnError` are the scheduler failures and shutdown
cancellations defined by the public Reminder contract.

## Tests and evidence

The conformance tests use `clock.NewFake`, `testing/synctest`, public Memory
stores, and test-owned storage adapters. They must not use sleeps, wall-clock
polling, private Runtime fields, or manual SQL repair.

The minimum test cases are:

1. Install generated bindings, register both Grain types, obtain typed
   References, and call the Device and fixed-key coordinator.
2. Set shadow State, assert `Exists`, clear it, and assert absence after
   reactivation. Also test a present zero value separately from absence.
3. Add Request Context, copy its value deliberately into Application data,
   and prove it is absent from Runtime State, Reminder data, and a Reminder
   Call.
4. Save an action, stop the Runtime before delivery, restart with the same
   stores, and recover the action without repair.
5. Run two concurrent public `Claim` attempts and assert one winner. A
   successful claim must precede the Device Call.
6. Stop after a successful claim and before delivery. Assert that the action
   remains pending and that the next periodic tick recovers it.
7. Fail before an application commit and assert `OnError`, one later retry,
   and one applied receipt.
8. Commit then return an injected unknown result. Retry the same ActionID and
   assert one applied receipt.
9. Invoke the same ApplyPending action twice and assert Safe Repeat behavior.
10. Cancel the recovery Reminder, clear coordinator State, and assert that no
    later tick runs.

The existing State, ReminderStore, Request Context, error-sink, and Runtime
restart tests remain lower-level evidence. The conformance tests prove their
composition. A test is not green because it only requested a run; it must
assert the observed Call, stored record, error source, and nonzero test count.

## Clean module and release gates

The generated file is committed. A clean consumer check must build the
committed example without access to the repository's build cache, then run
the two process phases against a temporary pair of database paths:

```bash
consumer_dir="$(mktemp -d)"
mod_cache="$consumer_dir/modcache"
(cd "$consumer_dir" && go mod init conformance-check)
GOMODCACHE="$mod_cache" go run github.com/suraciii/gor/examples/shadow/cmd/conformance@v0.1.0 \
    -phase prepare -db "$consumer_dir/runtime.db" -business-db "$consumer_dir/business.db"
GOMODCACHE="$mod_cache" go run github.com/suraciii/gor/examples/shadow/cmd/conformance@v0.1.0 \
    -phase recover -db "$consumer_dir/runtime.db" -business-db "$consumer_dir/business.db"
```

The prepare phase saves the action and exits with Reminder polling disabled.
The recover phase starts a new process with polling enabled and waits for the
observed recovery Call. It then reads the application receipt and exits with a
nonzero status if the receipt is missing or duplicated. The release check
uses the candidate version in place of `v0.1.0` before the tag exists.

The release candidate must pass every existing gate:

```text
make test
make sim
make gen
make net
make lint
go test -count=1 -race ./...
make ci
```

`make ci` is the required aggregate gate. It includes format checking, lint,
the default tests, race tests, simulation, generated-code tests, and network
tests. The clean consumer build and the two-phase run are additional release
evidence; they do not replace `make ci`.

## API decision and Gap

No new framework feature is required. Public `State`, `Reminder`, typed Grain
References, Request Context, `OnError`, `OnCall`, `Kill`, `New`, `Store`, and
`ReminderStore` provide all Runtime seams used here. The ApplicationStore is
application code because the Runtime must not own or interpret business
records.

The coordinator, ApplicationStore, process driver, generated bindings, and
conformance tests are implemented in `examples/shadow`. The existing shadow
`Device.Report` keeps its workshopID contract, so the conformance path uses the
separate typed `ReportAction` method for an ActionID. This keeps workshopID and
ActionID separate while preserving the Single Silo boundary. The example does
not configure cluster membership, a Transport, or private Runtime state.
