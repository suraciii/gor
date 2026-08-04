# 运行时

## 激活

一个实体的「激活」是它在某个节点内存里的实例。生命周期：

```
不存在 ──调用到达──▶ 激活中 ──▶ 活跃 ──空闲超时──▶ 停用中 ──▶ 不存在
                        │                              │
                    OnActivate                    OnDeactivate
                    从 store 读状态                 落盘
```

关键点：**用户从不显式创建或销毁实体。** `Ref[T](rt, key)` 只是构造一个引用，不触发任何 I/O。第一次方法调用才触发激活。

## 生命周期钩子

两个可选接口，实体实现哪个运行时就调哪个：

```go
type Activatable interface {
    OnActivate(ctx context.Context) error
}

type Deactivatable interface {
    OnDeactivate(ctx context.Context) error
}
```

**用可选接口，不用必填方法。** 大多数实体两个都不需要，让它们写两个空方法是纯粹的仪式；也不用注册期传函数——那会让「这个实体激活时做什么」离实体本身十万八千里。

`OnActivate` 在状态从 store 读回来之后、第一个调用进 mailbox 之前跑。**它返回错误就是激活失败**：触发这次激活的调用拿到这个错误，激活不建立，占位以错误 close，下一个调用重新来过。不给它「半激活」的中间态——一个 `OnActivate` 失败了却在服务的实体，比没有这个钩子更糟。

`OnDeactivate` 在实例即将消失之前跑，mailbox 已经排空。**它返回错误改变不了任何事**：停用不能被拒绝——空闲驱逐、panic 停用、视图变化卸载，三条路径都已经决定了这个实例要走，而且状态本来就在 store 里。所以它的错误没有调用方，跟定时投递失败一样交给运行时的错误出口（见 [timers.md](timers.md)），不做重试。

**`Kill()` 不调 `OnDeactivate`。** 崩溃不给收尾机会，这是 `Kill()` 存在的全部意义。

## 本地目录

每个节点维护一张表：

```go
type activation struct {
    id       Identity
    instance any
    mailbox  *mail.Box
    lastUsed time.Time
}
```

查找到达的请求：命中则投递到 mailbox；未命中则先激活。

**激活必须是幂等且互斥的**——同一个 key 的两个并发请求不能激活出两个实例。用一个 per-key 的「激活中」占位：第二个请求看到占位就等待，不重复激活。

```go
type entry struct {
    ready chan struct{}   // 激活完成后 close
    act   *activation     // ready 关闭后才可读
    err   error
}
```

这个模式是 Go 里的标准做法（等价于 `singleflight`），但**不用 `golang.org/x/sync/singleflight`**：它内部用 mutex，而 mutex 在 `synctest` 里不算 durably blocking，会让 bubble 无法判定静止。自己实现，用 channel。这是一条会反复出现的取舍：**能被 synctest 观测**优先于复用现成库。

## 空闲驱逐

一个后台循环定期扫描，把 `lastUsed` 超过阈值的激活停用。

驱逐必须与投递互斥：不能在停用过程中往它的 mailbox 投递新请求，否则请求会丢在一个即将死掉的 goroutine 上。做法是状态机——激活进入 `deactivating` 后，新请求走「重新激活」路径，等旧实例落盘完成。

时间来自注入的 `Clock`，因此这整块逻辑可以在假时钟下用单元测试穷举，不需要 `time.Sleep`。

## 请求分发

```
调用 ──▶ 环：这个 id 归谁？ ──是自己──▶ runtime ──▶ 本地目录 ──▶ mailbox
              （在 gor 里）        │
                                 是别人
                                   ▼
                             transport.Send ──▶ 远端节点
```

**分岔在 `gor` 里，不在 `runtime` 里。** `runtime` 只看得见左边那条：给它一个 Identity，它找到或建起激活，把调用投进 mailbox。它不知道右边存在，单节点模式下也就没有任何多余的代码要绕过（见 [cluster.md](cluster.md)）。

## 重入

默认：一个实体在处理一个调用时不接受第二个调用。

这带来经典的死锁场景——A 调 B，B 回头调 A。Orleans 用 `[Reentrant]` / `[AlwaysInterleave]` 标注放开这个限制，代价是用户要自己想清楚交错执行下的不变量。

`gor` 的取向：**先不提供重入。** 死锁在 `gor` 里表现为调用超时 + 一条明确的错误信息（指出检测到的调用环）。这比让用户在「加个标注解掉死锁」和「维护交错安全的不变量」之间做一个他很可能会做错的选择更好。

如果实践中证明必须要，再加，且要加在方法粒度而不是类型粒度。

**调用环检测**需要沿调用链传递一份「已占用的实体」集合。Go 里没有 `AsyncLocal`，只能显式塞进 `context.Context`——这是 Go 相对 .NET 的一处实打实的劣势，详见 [research/go-capabilities.md](../research/go-capabilities.md)。

## 错误与超时

每个调用带超时（来自 `ctx`）。超时的语义要说清：**超时意味着调用方不再等待，不意味着实体停止执行。** 方法体可能已经改了状态。

不提供自动重试。运行时不知道方法是否幂等，替用户重试会造成重复扣款那类问题。重试是调用方的决定。

## panic 处理

方法体 panic 时：recover、把 panic 转成 error 返回给调用方、**停用该激活**。

不继续用一个 panic 过的实例——它的内存状态可能已经不一致。下次调用重新从 store 恢复。这条选择的代价是 panic 会丢掉未落盘的内存状态，这是正确的方向。

## 停用时队列里还有调用

空闲驱逐发生时 mailbox 是空的——不空就不叫空闲。但 panic 触发的停用不挑时候，队列里可能还排着若干个没开始执行的调用。

规则：**排队未执行的调用一律以错误返回，不迁移到新实例。**

不迁移的理由是迁移要么重放导致同一个 panic 反复发生，要么需要判断「这个调用是不是刚才那个害死实例的」——而运行时没有这个信息。返回错误让调用方决定重不重试，跟「不提供自动重试」是同一条立场。

对调用方而言这跟超时是同一类事件：调用没有执行，状态没有改变。

## 两条停止路径

`Close()` 是优雅停机：关掉 mailbox，等在飞的调用做完，等驱逐循环退出。

`Kill()` 模拟崩溃：取消所有在飞调用的 context，关掉 mailbox，**不等排空**。排队未执行的调用以错误返回，跟停用时的规则一样。

两条都必须让运行时起过的 goroutine 全部退出。`Kill()` 少的是「等」，不是「收尾」——留一个还在跑的 goroutine 就等于泄漏，而模拟测试跑在 synctest 的 bubble 里，泄漏的 goroutine 会让整个 bubble 报死锁。

正在执行用户方法的调用杀不掉，Go 没有这个能力。`Kill()` 取消它的 context，方法体理不理是它自己的事。

`Kill()` 的存在理由只有模拟测试——真实进程崩溃不会先礼貌地调一个函数。它不是给用户用的关机接口，用户要停机用 `Close()`。
