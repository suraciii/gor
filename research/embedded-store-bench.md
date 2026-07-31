# 嵌入式存储实测

本文件记录批次 0 的实测事实和采集方法，不包含选型结论或推荐。

## 条件

- Machine: AMD Ryzen 9 9955HX 16-Core Processor, 1 socket, 16 cores, 32 threads.
- OS: Linux `pluto`, kernel `7.0.0-27-generic`, `x86_64`.
- Storage path: `/home/szf/repos/gor/.claude/worktrees/step2-persistence/.bench-data`.
- Filesystem: `ext4`, mounted `rw` from `/dev/mapper/ubuntu--vg-ubuntu--lv`; the backing device is LVM on `nvme0n1` (`CT2000P310SSD8`, `ROTA=0`).
- Go: `go1.26.5`, `linux/amd64`; `GOMAXPROCS=32`.
- Candidates: `modernc.org/sqlite v1.55.0`, `go.etcd.io/bbolt v1.5.0`, `github.com/cockroachdb/pebble/v2 v2.1.6`.
- Logical record data sizes: 256 B and 4 KiB.
- Seed size: 50,000 records per database.
- Timed samples: 5 per point-read, CAS-write, and concurrent-write result; 10 per cold-open result.
- Workload operations: 100,000 point reads and 4,096 writes per timed sample. The write count is divisible by 1, 8, and 64.
- Timed runs use `time.Now()` around the complete operation loop. Reported `ns/op` is elapsed sample time divided by operation count.
- Each workload uses a newly seeded database. The OS page cache was not cleared between samples or candidates.

Backend settings:

| Backend | WAL / durability setting | Connection or writer setting |
|---|---|---|
| SQLite | `journal_mode=WAL`, `synchronous=FULL` | `busy_timeout=60000 ms`; `database/sql` max open and idle connections 64 |
| bbolt | No separate WAL; default `NoSync=false` | Default mmap and transaction settings; open timeout 1 minute |
| Pebble | Default WAL; `WriteOptions{Sync:true}` for every write batch | Default Pebble options; logging disabled for benchmark output |

The SQLite store uses a table with `id BLOB PRIMARY KEY`, `data BLOB`, and `etag INTEGER`. bbolt stores records in a `records` bucket. Pebble uses the record identity as the key. The bbolt and Pebble values contain an 8-byte big-endian ETag followed by the data payload.

## Method

Point read reads the pre-existing `entity-00000` key. Each CAS-write operation reads the current record, then writes the same-size payload with the current ETag as the expected ETag and the ETag incremented by one. Each write is one committed transaction or batch.

The concurrent-write workload starts exactly 8 or 64 workers. Each worker repeatedly performs the same read-then-CAS sequence on its own pre-existing key, so workers do not contend on the same identity. The CAS implementations validate the expected ETag before committing.

### Pebble CAS behavior

Pebble has no conditional-update primitive matching SQLite's `UPDATE ... WHERE etag` or bbolt's writable transaction. Its benchmark path creates an indexed batch, reads the current value through that batch, checks the ETag, sets the value, and commits the batch with `Sync:true`. The batch commit is atomic for the operations in that batch, but the ETag check is not atomic with respect to the current database value. If two batches write the same key concurrently, both can read the same ETag and both can commit successfully. The benchmark uses a distinct key per worker, so this same-key behavior is not exercised by the concurrent-write numbers.

Each sequential workload has 100 warm-up operations. Concurrent workloads warm one key per worker before timing. The cold-open workload seeds and closes a database, then measures `Open` plus the backend initialization needed by the benchmark: SQLite includes `Ping` and prepared statement creation; bbolt and Pebble include their normal open path. It closes the database after every cold-open sample.

The benchmark source and module are outside this repository at `/tmp/gor-storebench/`. The formal command was:

```text
GOCACHE=/tmp/gocache GOPROXY=off go run . -data-dir /home/szf/repos/gor/.claude/worktrees/step2-persistence/.bench-data -runs 5 -point-ops 100000 -write-ops 4096 -cold-samples 10
```

The seed operation used one transaction for SQLite, one writable transaction for bbolt, and one synchronous batch commit for Pebble. The benchmark source passed `go test ./...` before the formal run.

The `.bench-data` directory was removed after the formal run.

## Results

All values are elapsed nanoseconds per operation. `range` is the minimum-to-maximum sample range. `median ops/s` is derived from the median value.

### Point read

| Backend | Data | Concurrency | Median ns/op | Range ns/op | Median ops/s |
|---|---:|---:|---:|---:|---:|
| SQLite | 256 B | 1 | 4,296 | 4,094-4,368 | 232,774.67 |
| SQLite | 4 KiB | 1 | 6,152 | 5,190-6,629 | 162,548.76 |
| bbolt | 256 B | 1 | 499 | 453-513 | 2,004,008.02 |
| bbolt | 4 KiB | 1 | 533 | 498-621 | 1,876,172.61 |
| Pebble | 256 B | 1 | 701 | 687-727 | 1,426,533.52 |
| Pebble | 4 KiB | 1 | 682 | 679-683 | 1,466,275.66 |

### CAS write

| Backend | Data | Concurrency | Median ns/op | Range ns/op | Median ops/s |
|---|---:|---:|---:|---:|---:|
| SQLite | 256 B | 1 | 1,728,721 | 1,708,248-1,758,435 | 578.46 |
| SQLite | 4 KiB | 1 | 3,948,900 | 3,828,952-4,613,159 | 253.24 |
| bbolt | 256 B | 1 | 1,729,617 | 1,722,521-2,178,839 | 578.16 |
| bbolt | 4 KiB | 1 | 1,762,374 | 1,730,876-1,909,423 | 567.42 |
| Pebble | 256 B | 1 | 3,969,279 | 3,806,799-4,454,872 | 251.93 |
| Pebble | 4 KiB | 1 | 871,199 | 852,571-1,602,912 | 1,147.84 |

### Concurrent write, 8 workers

| Backend | Data | Concurrency | Median ns/op | Range ns/op | Median ops/s |
|---|---:|---:|---:|---:|---:|
| SQLite | 256 B | 8 | 1,759,628 | 1,742,361-1,848,924 | 568.30 |
| SQLite | 4 KiB | 8 | 1,944,184 | 1,819,134-3,919,970 | 514.35 |
| bbolt | 256 B | 8 | 3,771,531 | 3,532,389-3,922,368 | 265.14 |
| bbolt | 4 KiB | 8 | 1,696,086 | 1,675,077-1,743,060 | 589.59 |
| Pebble | 256 B | 8 | 417,302 | 410,623-1,014,412 | 2,396.35 |
| Pebble | 4 KiB | 8 | 225,535 | 212,398-429,257 | 4,433.90 |

### Concurrent write, 64 workers

| Backend | Data | Concurrency | Median ns/op | Range ns/op | Median ops/s |
|---|---:|---:|---:|---:|---:|
| SQLite | 256 B | 64 | 2,479,072 | 2,332,550-4,129,085 | 403.38 |
| SQLite | 4 KiB | 64 | 2,575,614 | 2,331,726-2,955,210 | 388.26 |
| bbolt | 256 B | 64 | 3,370,753 | 1,670,599-4,131,339 | 296.67 |
| bbolt | 4 KiB | 64 | 3,540,376 | 1,691,051-3,969,747 | 282.46 |
| Pebble | 256 B | 64 | 55,210 | 53,501-55,944 | 18,112.66 |
| Pebble | 4 KiB | 64 | 34,247 | 33,242-62,704 | 29,199.64 |

### Cold open, 50,000 existing records

| Backend | Data | Samples | Median ns/open | Range ns/open | Median opens/s |
|---|---:|---:|---:|---:|---:|
| SQLite | 256 B | 10 | 149,731 | 137,448-367,119 | 6,678.64 |
| SQLite | 4 KiB | 10 | 148,839 | 134,292-880,782 | 6,718.67 |
| bbolt | 256 B | 10 | 14,271 | 13,184-83,456 | 70,072.17 |
| bbolt | 4 KiB | 10 | 8,511 | 7,965-72,737 | 117,495.01 |
| Pebble | 256 B | 10 | 8,980,036 | 8,703,793-9,645,750 | 111.36 |
| Pebble | 4 KiB | 10 | 8,980,722 | 8,779,245-11,242,174 | 111.35 |
