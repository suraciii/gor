# Device shadow example

A directly runnable device-shadow service: devices report state, the service keeps the last report; after more than 30 seconds without a new message, the device goes offline and its workshop's online count updates.

## Why these things are Grains

In the example, `Device` and `Workshop` are Grain types. Each Grain has a
stable GrainId and State that must be updated serially.
The four points below all correspond to the domain code in `domain/domain.go`.

- **Large device count, mostly idle.** `Device` uses the device key as its
  GrainKey. Its State uses `gor.State`. The Runtime can evict idle
  Activations while State stays in the store.
- **Writes to one device must be serialized.** Reports and configuration
  change the Device Grain without locks. Calls for the same GrainId are
  queued and executed by `gor`.
- **Offline is a one-shot Reminder.** Each report resets the `offline`
  Reminder. When it runs, it marks the Device Grain offline and notifies the
  Workshop Grain.
- **Online count is a cross-Grain aggregation.** Devices notify `Workshop`
  when they go online, change workshops, or go offline. The Workshop Grain
  keeps GrainIds. It does not call every Device Grain one by one.

Each GrainId has its own Activation, State, and serialized Calls.

## Running it

Run from the gor repository root:

```bash
go run ./examples/shadow/cmd/shadow
```

Registering the shadow Grains only needs the Runtime:

```go
if err := shadow.Register(rt); err != nil {
    return err
}
```

Reminders and lifecycle hooks have no requester waiting. When starting the
Runtime, install the unified error sink, or these errors are dropped:

```go
rt, err := gor.New(
    gor.WithStore(database),
    gor.OnError(shadow.LogBackgroundError),
)
```

The service listens on `:8080` and writes data to `data/gor.db`. Address and database file are configurable:

```bash
go run ./examples/shadow/cmd/shadow -addr :9090 -db ./data/shadow.db
```

## Running it on multiple nodes

The same service runs as a cluster. Start one process per node, all sharing one database file; each node takes a distinct HTTP address and cluster transport address, and a fresh membership generation is taken automatically on every start:

```bash
go run ./examples/shadow/cmd/shadow -cluster -addr 127.0.0.1:8081 -node-addr 127.0.0.1:7371 -db ./data/cluster.db
go run ./examples/shadow/cmd/shadow -cluster -addr 127.0.0.1:8082 -node-addr 127.0.0.1:7372 -db ./data/cluster.db
go run ./examples/shadow/cmd/shadow -cluster -addr 127.0.0.1:8083 -node-addr 127.0.0.1:7373 -db ./data/cluster.db
```

Run each in its own terminal. Every node serves the same HTTP API. A request
is executed on the Silo that owns its Grain. The Grain definitions and
handlers are the same as in the single-Silo service.

A node begins serving when it joins. As Silos discover each other, Grain
ownership settles. During that window the same Grain may be active on two
Silos. One State write can then fail with a conflict. This does not happen in
the single-Silo service. The full boundary is in
[../../docs/programming-model.md](../../docs/programming-model.md).

## Calling it

Report from device `device-1` to the `assembly` workshop:

```bash
curl -i -X POST http://localhost:8080/devices/device-1/reports \
  -H 'Content-Type: application/json' \
  -d '{"workshop_id":"assembly","state":"temperature=20"}'
```

Succeeds with `204 No Content`. Push configuration:

```bash
curl -i -X PUT http://localhost:8080/devices/device-1/configuration \
  -H 'Content-Type: application/json' \
  -d '{"configuration":"sample-rate=10s"}'
```

Read the device shadow:

```bash
curl http://localhost:8080/devices/device-1/shadow
```

The response contains the last reported state, report time, online status, workshop, and configuration, for example:

```json
{
  "reported_state": "temperature=20",
  "reported_at": "2026-08-04T12:00:00Z",
  "online": true,
  "workshop_id": "assembly",
  "configuration": "sample-rate=10s"
}
```

Read a workshop's online count:

```bash
curl http://localhost:8080/workshops/assembly/online-count
```

```json
{"online_count":1}
```

Every report resets a 30-second one-shot alarm. Stop reporting and wait 30 seconds, then reading the shadow again shows `online` has become `false`, and the workshop online count has become `0`. After a one-shot alarm fires, it is deleted from the schedule table; the next report brings the device back online and creates a new alarm.

## Watching idle eviction

The load generator is not a performance test; it reports no throughput or latency. It uses a temporary SQLite, has a batch of devices report concurrently, then pushes configuration to `device-000`; it waits for the runtime to evict idle activations, then re-reads its shadow:

```bash
go run ./examples/shadow/cmd/load
```

The program waits on `OnDeactivate` channel signals for all devices and workshops to be evicted for idleness, then asserts the local activation directory is empty; it then reads the shadow, triggering `OnActivate` to reload from the store, and confirms the configuration is still readable. The program deletes the temp directory on exit. Use `-devices` to adjust the batch size.
