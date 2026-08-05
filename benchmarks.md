# Benchmark 基线

测量日期：2026-08-05。数字使用默认 `benchtime`，保留两位有效数字。

| Benchmark | 结果 | 存储后端 | fsync |
| --- | ---: | --- | --- |
| 调用往返 | 0.89 us/op | `store.NewMemory()` | 不适用，无磁盘 I/O |
| 状态写入 | 1.8 ms/op | 真盘 SQLite（WAL） | 开启，`synchronous=FULL` |
| 冷激活 | 18 us/op | 真盘 SQLite（WAL） | 开启，`synchronous=FULL` |

## 测量条件

- CPU：AMD Ryzen 9 9955HX 16-Core Processor，16 个物理核、32 个逻辑 CPU。
- 盘：CT2000P310SSD8 NVMe SSD，非旋转盘；benchmark 目录位于 ext4 的 `/dev/mapper/ubuntu--vg-ubuntu--lv`。
- Go：`go1.26.5 linux/amd64`。
- 并发度：每个 benchmark 一个实体、一个 key、单 goroutine 串行调用；未使用并发 worker。
- 正式复现应在空闲机器上取数，不要同时运行编译或测试；后台负载会明显抬高冷激活，尤其是同时有编译和测试时可能接近翻倍。本次记录时没有活跃的 Go/.NET 编译或测试命令，但系统仍有一个独立的长期 `VBCSCompiler` 服务，未对它做终止操作。
- 状态写入和冷激活使用同一类真盘 SQLite 配置：WAL，`synchronous=FULL`。
- 冷激活开始前由实体的 `Seed` 写入一份 `gor.State`；实体被驱逐后，计时的第一次调用会在 Factory 的 `binder.load(ctx)` 中从盘上读回这份状态。这个盘上读回包含在冷激活数字内；方法本身不额外写状态。
- 每次 benchmark 使用工作区下的 `.gor-bench-*` 临时目录。`GOR_BENCH_DIR` 必须指向真盘目录；tmpfs 和 ramfs 会被守卫拒绝。

## 复现命令

下面命令不指定 `-benchtime`，因此使用 Go benchmark 的默认值；运行前应确保当前目录或 `GOR_BENCH_DIR` 位于真盘上。

```sh
GOR_BENCH_DIR="$PWD" go test . -run '^$' -bench . -count=1
```
