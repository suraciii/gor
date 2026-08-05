# Benchmark baseline

Measured on 2026-08-05. Numbers use the default `benchtime`, rounded to two significant figures.

| Benchmark | Result | Storage backend | fsync |
| --- | ---: | --- | --- |
| Invocation round trip | 0.89 us/op | `store.NewMemory()` | N/A, no disk I/O |
| State write | 1.8 ms/op | SQLite on real disk (WAL) | on, `synchronous=FULL` |
| Cold activation | 18 us/op | SQLite on real disk (WAL) | on, `synchronous=FULL` |

## Observability and forwarding

Measured on 2026-08-05. The results below come from a single `make bench` run, with the default `benchtime` and `-benchmem`.

| Benchmark | ns/op | allocs/op | Conditions |
| --- | ---: | ---: | --- |
| `BenchmarkInvocationRoundTrip` | 885.1 | 10 | in-memory store, `OnCall` disabled |
| `BenchmarkInvocationRoundTripWithOnCall` | 951.5 | 10 | in-memory store, empty `OnCall` callback |
| `BenchmarkForwardingRoundTrip/Local` | 883.8 | 10 | in-memory store, local `Noop` |
| `BenchmarkForwardingRoundTrip/Forwarded` | 21397 | 45 | in-memory store, real loopback TCP forwarded `Noop` |

With observation disabled, results are about `-0.5%` relative to the existing `0.89 us/op` baseline; an empty callback adds `7.5%` over the disabled state. Forwarding adds about `20513 ns/op` over a same-condition local call — about `24.2` times a local call. The forwarding row uses real loopback TCP to include 6b's framing, JSON encoding/decoding, connection reuse, and handler path; the local and forwarded calls invoke the same `Noop` method on the same type, and the connection and activation are warmed up before timing. This number is not machine-to-machine network latency; it only answers the library-internal extra cost of forwarding over local.

## Measurement conditions

- CPU: AMD Ryzen 9 9955HX 16-Core Processor, 16 physical cores, 32 logical CPUs.
- Disk: CT2000P310SSD8 NVMe SSD, non-rotating; the benchmark directory is on ext4 at `/dev/mapper/ubuntu--vg-ubuntu--lv`.
- Go: `go1.26.5 linux/amd64`.
- Concurrency: one entity, one key, single-goroutine serial calls per benchmark; no concurrent workers.
- For formal reproduction, take numbers on an idle machine; do not run builds or tests at the same time. Background load raises cold activation noticeably — with concurrent build and test activity it can approach doubling. At the time of measurement no Go/.NET build or test commands were active, but an independent long-running `VBCSCompiler` service was present; it was not terminated.
- State write and cold activation use the same real-disk SQLite configuration: WAL, `synchronous=FULL`.
- The observability benchmarks use one entity, one key, single-goroutine serial calls; the forwarding benchmarks use two in-process runtimes, in-memory stores, and two real loopback TCP listeners.
- Before cold activation starts, the entity's `Seed` writes a `gor.State`; after the entity is evicted, the first timed call reads this state back from disk in the factory's `binder.load(ctx)`. That disk read-back is included in the cold-activation numbers; the method itself writes no additional state.
- Each benchmark uses a `.gor-bench-*` temp directory under the working directory. `GOR_BENCH_DIR` must point at a real-disk directory; tmpfs and ramfs are rejected by a guard.

## Reproduction commands

```sh
make bench
```

Do not set `-benchtime`; use Go benchmark defaults. If the working directory is on a real disk, run it as-is; on tmpfs (for example, the repo under `/tmp`), point at a real disk with `GOR_BENCH_DIR=/some/real/disk make bench`.
