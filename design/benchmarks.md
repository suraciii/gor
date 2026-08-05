# Performance baseline

## Why it exists

Two purposes; neither is about pretty numbers.

**Inward**: spot that a change made things slower. Without a baseline, a performance regression is only discovered when a user reports it.

**Outward**: users must be able to judge whether this library fits their scenario. For a library that cannot say how fast it is, users have to measure it themselves, or simply not use it.

## Three measurements

**Invocation round trip** — in-memory store, single process, serial calls to a method that does nothing on one entity. It measures the runtime's own overhead: mailbox in and out, activation lookup, reflection dispatch. Mix in storage and it cannot be measured.

**State write** — real disk; how long one `Set()` takes to land. This is the number users care about most, because it decides how many state changes per second one entity can do.

**Cold activation** — after an entity is evicted, how long the first call takes. The core promise of virtual entities is "you do not manage the lifecycle"; this number states the price of that sentence.

This one needs a real-disk store, and the entity must carry state that was written before. Being evicted means the state went back to disk, and reading it back is the bulk of this cost. An in-memory store would measure the runtime's lookup-and-construct overhead, not the time the user waits.

Each of the three gets its own benchmark; no composite score. A composite score hides exactly the only useful information: which layer is slow.

## What is not measured

- **No comparison with Orleans.** The runtimes, GCs, and serialization all differ; the resulting numbers explain nothing and become a freely quotable marketing line.
- **No per-node QPS.** It depends on what the user's method bodies do; it has nothing to do with the library.
- **Cross-node forwarding is not merged into the three above.** [6b](../ROADMAP.md#6b-转发) is implemented; measure "how much more expensive forwarding is than local" separately, rather than compositing it with single-process absolute numbers.

## Every number must carry its measurement conditions

A number without conditions is fake. Every result must carry: machine (CPU, core count), disk (model, or at least "SSD / HDD / virtual disk"), Go version, storage backend, fsync on or off, concurrency.

**Storage benchmarks must not run on tmpfs.** On many machines `/tmp` is tmpfs, where `fsync` is a no-op; measured write latency comes out two to three orders of magnitude faster than on a real disk, and the whole set of numbers is void. Use a directory on a real disk and state in the conditions that it is one.

This is not cleanliness: one `fsync` is milliseconds on a real disk and microseconds on tmpfs. Numbers three orders of magnitude apart make every judgment based on them wrong.

**What is blocked is memory filesystems, not "only one kind of disk is accepted".** tmpfs and ramfs are rejected; everything else is allowed through. A whitelist of approved filesystems would stop people on xfs, btrfs, or zfs from running benchmarks, and their disks are real: the kinds of real disks form an open set; memory filesystems are just those two.

## Where the numbers live

One file, moving with the code. Big changes get re-measured; re-measuring overwrites; no history is kept — history belongs to git log.

**The baseline is not a CI gate.** Gating on numbers only breeds flakiness: two runs on the same machine can differ by ten percent, let alone on shared CI machines. It is for humans to read, not for machines to enforce.

Regression detection is by human review: a PR touching the hot path re-measures, and if the numbers moved, the description says so.
