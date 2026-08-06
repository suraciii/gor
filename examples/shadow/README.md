# Device shadow example

A directly runnable device-shadow service: devices report state, the service keeps the last report; after more than 30 seconds without a new message, the device goes offline and its workshop's online count updates.

## Why these things are entities

In the example, `Device` and `Workshop` both have stable identities, and each owns a set of state that must be updated serially.
The four points below all correspond to the domain code in `domain/domain.go`.

- **Large device count, mostly idle.** `Device` uses the device id as its identity, with state in `gor.State`; the runtime can evict idle activations from memory, while state stays in the store.
- **Writes to one device must be serialized.** Reports and configuration both change the device's own shadow directly, with no locks; calls on the same identity are queued and executed by `gor`. Readers can see both entry points directly on the `Device` interface.
- **Offline is a one-shot scheduled task that follows the device.** Every report resets the `offline` alarm; when the task fires, it marks the device offline and notifies the workshop.
- **Online count is a cross-entity aggregation.** Devices notify `Workshop` proactively on going online, changing workshops, and going offline; the workshop only keeps the identity set of online devices — it does not hold device references and query them one by one.

These are not about splitting the code into more types; each identity needs its own activation, state, and serialized calls.

## What gor does not provide

Shadow write, offline-alarm reset, and workshop notification are three independent operations. `gor` provides no cross-entity transactions and no compensation or rollback for the user: if the shadow write succeeds and a later alarm or notification fails, the device and workshop can be temporarily inconsistent. The example returns the error to the caller and leaves the window for the business to decide whether it is acceptable; the implementation order is in the `Report` method of `domain/domain.go`.

## Running it

Run from the gor repository root:

```bash
go run ./examples/shadow/cmd/shadow
```

Registering the shadow entities only needs the runtime:

```go
if err := shadow.Register(rt); err != nil {
    return err
}
```

Scheduled tasks and lifecycle hooks have no requester waiting. When starting the runtime, install the unified error sink, or these two kinds of errors are dropped:

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

Run each in its own terminal. Every node serves the same HTTP API; send a request to any node and it is executed on the node that owns that entity, forwarded over the cluster transport when that node is a different one. The entity definitions and handlers are identical to the single-node service — clustering is a launcher concern, not a business-code one.

A node begins serving the moment it joins. As the nodes discover each other, which node owns which entity settles within about a second; during that brief window an entity may be served by a node that is about to hand it off, but it is always served, from the shared state. `curl` any node the same way as the single-node service.

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
