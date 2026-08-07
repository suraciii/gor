# Benchmark baseline

Measured on 2026-08-05. Numbers use the default `benchtime`, rounded to two significant figures.

| Benchmark | Result | Storage backend | fsync |
| --- | ---: | --- | --- |
| Invocation round trip | 0.89 us/op | `store.NewMemory()` | N/A, no disk I/O |
| State write (Full) | 1.7 ms/op | SQLite on real disk (WAL) | on, `synchronous=FULL` |
| State write (Relaxed) | 14 us/op | SQLite on real disk (WAL) | checkpoint only, `synchronous=NORMAL` |
| Cold activation | 18 us/op | SQLite on real disk (WAL) | on, `synchronous=FULL` |

## Observability and forwarding

Measured on 2026-08-05. The results below come from a single `make bench` run, with the default `benchtime` and `-benchmem`.

| Benchmark | ns/op | allocs/op | Conditions |
| --- | ---: | ---: | --- |
| `BenchmarkInvocationRoundTrip` | 885.1 | 11 | in-memory store, `OnCall` disabled |
| `BenchmarkInvocationRoundTripWithOnCall` | 951.5 | 11 | in-memory store, empty `OnCall` callback |
| `BenchmarkForwardingRoundTrip/Local` | 883.8 | 11 | in-memory store, local `Noop` |
| `BenchmarkForwardingRoundTrip/Forwarded` | 21397 | 48 | in-memory store, real loopback TCP forwarded `Noop` |

With observation disabled, results are about `-0.5%` relative to the existing `0.89 us/op` baseline; an empty callback adds `7.5%` over the disabled state. Forwarding adds about `20513 ns/op` over a same-condition local call — about `24.2` times a local call. The forwarding row uses real loopback TCP to include 6b's framing, JSON encoding/decoding, connection reuse, and handler path; the local and forwarded calls invoke the same `Noop` method on the same type, and the connection and activation are warmed up before timing. This number is not machine-to-machine network latency; it only answers the library-internal extra cost of forwarding over local.

Re-verified on 2026-08-06: the ns/op values are unchanged code-side. The machine was under sustained load (load average ≈ 17, including an inference server at ≈ 1500% CPU), so absolute numbers came out about 2.3 times higher; an A/B comparison of the baseline commit `0678933` with the current HEAD under that identical load shows overlapping distributions (local 2.0–2.2 us, forwarded 41–48 us), so the recorded idle-machine values remain the formal baseline. Allocations did move: +1 per local call and +3 per forwarded call (624 B and 2199 B total before, 640 B and 2250 B now), from the admission gate and the error-envelope work that landed after the original baseline. `BenchmarkStateWrite` reproduces at 1.8 ms/op unchanged; cold activation measured 40 us/op under load against 18 us/op recorded, consistent with its documented load sensitivity.

## Measurement conditions

- CPU: AMD Ryzen 9 9955HX 16-Core Processor, 16 physical cores, 32 logical CPUs.
- Disk: CT2000P310SSD8 NVMe SSD, non-rotating; the benchmark directory is on ext4 at `/dev/mapper/ubuntu--vg-ubuntu--lv`.
- Go: `go1.26.5 linux/amd64`.
- Concurrency: one entity, one key, single-goroutine serial calls per benchmark; no concurrent workers.
- For formal reproduction, take numbers on an idle machine; do not run builds or tests at the same time. Background load raises cold activation noticeably — with concurrent build and test activity it can approach doubling. At the time of measurement no Go/.NET build or test commands were active, but an independent long-running `VBCSCompiler` service was present; it was not terminated.
- State write and cold activation use the same real-disk SQLite configuration: WAL. The Full tier and cold activation run `synchronous=FULL`; the Relaxed tier runs `synchronous=NORMAL`, which in WAL mode syncs at checkpoint instead of on every commit.

Re-measured on 2026-08-07 (store-level, `store/benchmark_test.go`): the State write rows are re-measurements on this machine under its usual background load — long-running services are present, so these are not clean-idle numbers. At run time the load average was ≈ 1.0 on 32 logical CPUs and no Go build or test was active. Full measured 1.74 ms/op, inside the band of the frozen 1.8 ms/op baseline (three 2 s runs the same day span 1.71–2.09 ms/op); it replaces the baseline as a re-measurement under ambient load, not as a cleaner one. Relaxed measured 13.8 us/op. Both rows were measured with the default `benchtime` on ext4 via `GOR_BENCH_DIR=/home/szf/gor-bench` (statfs magic 0xef53, not tmpfs).
- The observability benchmarks use one entity, one key, single-goroutine serial calls; the forwarding benchmarks use two in-process runtimes, in-memory stores, and two real loopback TCP listeners.
- Before cold activation starts, the entity's `Seed` writes a `gor.State`; after the entity is evicted, the first timed call reads this state back from disk in the factory's `binder.load(ctx)`. That disk read-back is included in the cold-activation numbers; the method itself writes no additional state.
- Each benchmark uses a `.gor-bench-*` temp directory under the working directory. `GOR_BENCH_DIR` must point at a real-disk directory; tmpfs and ramfs are rejected by a guard.

## Reproduction commands

```sh
make bench
```

Do not set `-benchtime`; use Go benchmark defaults. If the working directory is on a real disk, run it as-is; on tmpfs (for example, the repo under `/tmp`), point at a real disk with `GOR_BENCH_DIR=/some/real/disk make bench`.
