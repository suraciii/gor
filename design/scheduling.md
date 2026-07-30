# 调度

## 目标

同一实体上的调用严格串行；不同实体之间完全并行。

## 实现

一个实体一个 mailbox：一个 goroutine 从一个 channel 读，循环执行。

```go
type Box struct {
    in     chan *call
    done   chan struct{}
}

type call struct {
    fn    func(ctx context.Context) (any, error)
    reply chan result   // 容量 1
    ctx   context.Context
}

func (b *Box) run() {
    for c := range b.in {
        v, err := c.fn(c.ctx)
        c.reply <- result{v, err}
    }
    close(b.done)
}
```

串行性由「只有一个 goroutine 在跑循环体」直接给出。没有锁，没有自定义调度器。

**`reply` 必须有容量 1。** 调用方超时后就不再读这个 channel 了（[runtime.md](runtime.md)：超时意味着调用方不再等待，不意味着实体停止执行）。如果 `reply` 无缓冲，这次发送会永远挂住 —— 不是挂住一个调用，是挂住整个 mailbox 的循环，这个实体从此死了。容量 1 让发送永不阻塞，结果没人取就随 channel 一起被回收。

不用 `select` 加 `ctx.Done()` 代替：那样每次回复都要多写一个分支，而它要防的事情用一个缓冲位就防住了。

预估整个 `mail` 包在 100 行量级。对比：Orleans 的 `Scheduler/` 实测 823 行，其中 `WorkItemGroup` 336 行——因为 .NET 需要实现一个自定义 `TaskScheduler` 来保证 `await` 之后回到同一个逻辑执行上下文。Go 里 goroutine 天然是执行上下文，这个问题不存在。

## channel 容量

用无缓冲还是有缓冲，直接影响背压语义：

- **无缓冲**：调用方阻塞到实体开始处理。背压自动传导，但一个慢实体会把调用方全部挂住。
- **有缓冲**：吸收突发，但缓冲满了以后行为要定义清楚——阻塞还是拒绝。

选择：**有界缓冲，满了拒绝**（返回一个明确的过载错误）。理由是无界队列会把内存问题伪装成延迟问题，而阻塞会让一个热点实体拖垮整个进程。容量可配置。

## 请求顺序

同一调用方对同一实体的连续调用，按发起顺序执行——channel 是 FIFO，本地场景下天然成立。

**跨节点不保证顺序。** 网络重排 + 重连会破坏它，而为此加序号和重排缓冲的复杂度不值得。文档要明说这条，不要让用户以为有顺序保证。

## 与 synctest 的关系

这个设计能被 `testing/synctest` 完整观测，这不是巧合而是选择的理由：

- goroutine 阻塞在 channel 上算 "durably blocked"，`synctest.Wait()` 能判定系统静止。
- 如果改用 mutex + 条件变量实现串行化，`synctest` **无法**判定静止（mutex 阻塞不算 durably blocking），整个测试策略就塌了。

所以 `mail` 包里禁止出现 `sync.Mutex` 用于跨调用的等待。纯粹保护一个 map 的短临界区是可以的，但不能用它来「等」。

## 与定时任务的关系

持久化的定时任务（`Schedule`）不走 mailbox 的时钟。它是「一张表 + 一个轮询器」：轮询器发现到期项，就构造一个普通调用投递给目标实体的 mailbox。

也就是说定时任务在实体看来跟普通方法调用没有区别，同样享受串行保证。

**明确不重复 Orleans Reminders v1 的设计**——那套是内存缓存 + 环形分区 + 复杂的所有权转移，Orleans 自己在 `Orleans.DurableJobs`（v2，实测 5278 行，仍是 preview）里换掉了。表 + 轮询更笨但更容易验证，精度也够——持久化定时任务本来就不该承诺毫秒级。
