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
    OnDeactivate(ctx context.Context, reason DeactivationReason) error
}

type DeactivationReason uint8

const (
    Idle DeactivationReason = iota + 1
    OwnershipLost
    RuntimeClosed
    Faulted
)
```

**用可选接口，不用必填方法。** 大多数实体两个都不需要，让它们写两个空方法是纯粹的仪式；也不用注册期传函数——那会让「这个实体激活时做什么」离实体本身十万八千里。

`OnActivate` 在状态从 store 读回来之后、第一个调用进 mailbox 之前跑。**它返回错误就是激活失败**：触发这次激活的调用拿到这个错误，激活不建立，占位以错误 close，下一个调用重新来过。不给它「半激活」的中间态——一个 `OnActivate` 失败了却在服务的实体，比没有这个钩子更糟。

`OnDeactivate` 在实例即将消失之前跑，mailbox 已经排空。它收到首次开始停用的 `DeactivationReason`。原因只有四种：

| 原因 | 首次停用的触发 | 应用可据此做的事 |
| --- | --- | --- |
| `Idle` | 实例空闲超时。 | 不把一次本地回收当成业务对象离线。 |
| `OwnershipLost` | 当前节点不再拥有身份，或视图没有 active owner。 | 释放节点本地的 lease 或连接，不宣告业务对象消失。 |
| `RuntimeClosed` | 根运行时开始正常关闭。 | 做进程退出前的收尾。 |
| `Faulted` | 方法 panic，或实体要求丢弃当前实例。 | 不把不可信实例当作正常告别，并提高告警级别。 |

这是完整的公开集合。一个值必须让应用作出不同决定，才可以加入这个集合；不能因为实现里多一条分支就增加原因。panic 和丢弃都表示当前实例不再可信；迁移和无 owner 都表示当前节点失去责任，所以各自只有一个值。

原因在 `beginDeactivation(reason)` 把 activation 从 `active` 变为 `deactivating` 的同一原子转换中写入。后续调用在 activation 已经停用时不得改写它。比如根运行时已经进入 `closing`，其中一个 activation 可能早已因 `Idle` 开始停用；它的原因仍是 `Idle`。停用原因描述一个 activation 首次为何离场，根状态机描述整个运行时是否接纳调用、怎样等待和以何种停止错误拒绝调用。这是两套概念，不能共用一个枚举。

**它返回错误改变不了任何事**：停用不能被拒绝，而且状态本来就在 store 里。错误没有调用方，跟定时投递失败一样交给运行时的错误出口（见 [timers.md](timers.md)），不做重试。出口的来源带 `Deactivation{Reason: reason}`，不再伪造方法名。

每次正常停用钩子获得新的 `context.Background()`。这个 context 没有 deadline，也不会取消；它不继承任何实体调用者的 context。正常关闭会等待已经开始的钩子返回，因此钩子必须尽快完成。

**`Kill()` 和本节点被判 dead 都不启动 `OnDeactivate`。** 突发停止不给尚未开始的收尾机会。已经开始的钩子不被取消，突发停止也不等待它；给它一个已取消的 context 只会制造一条部分收尾的第三种语义。

这一条要盖住已经在飞的那些停用。空闲驱逐先动手、`Kill()` 后到，钩子还没跑的实例一样不许跑——判断标准是钩子跑之前这个实例有没有被杀，不是这次停用由谁发起。所以「跳过」是记在实例上的一个事实，不是某条停用路径的参数：路径可以有好几条，实例只有一个。

### 迁移

这是计划内的 v0 不兼容变更。已有 `Deactivatable` 实现把停用方法改为接收第二个 `DeactivationReason` 参数。实现不得用默认分支把未知原因当成正常停用；运行时只会传上表四个值。

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

## 根运行时停止状态机

根运行时拥有调用接纳和停止原因。内部执行运行时只拥有激活和 mailbox；集群节点只拥有成员状态；轮询器和传输都不是另一套停止开关。根状态机如下：

```
running ── Close ──▶ closing ── 完成优雅停止 ──▶ stopped
   │                    │
   │                    └── Kill ──▶ killing ── 完成突发停止 ──▶ stopped
   │
   ├── Kill ──▶ killing
   │
   └── 本节点被判 dead ──▶ dead ── 完成突发停止 ──▶ stopped
```

状态转换只有四个函数：`beginClose` 把 `running` 变为 `closing`；`beginKill` 把 `running` 或 `closing` 变为 `killing`；`becomeDead` 只把仍在 `running` 的根运行时变为 `dead`；`finishStop` 把 `closing`、`killing` 或 `dead` 变为 `stopped`。没有回边。重复的 `Close` 和已经停下后的 `Kill` 不改变状态；`closing` 中的 `Kill` 是升级，不是等待已有 `Close` 的 no-op。

### 接纳是唯一边界

`beginClose`、`beginKill` 或 `becomeDead` 成功离开 `running` 的那个原子转换，就是调用接纳的线性化点。它同时关闭公开的停止信号。信号关闭不是资源已经全部释放的证明；它只证明此后不能再接纳一次实体调用。

每次实体调用先经过根层的 `admit`。它在与状态转换相同的串行域中同时检查 `running` 并登记这次调用，返回一个完成时必须调用的 release。一次 `admit` 要么排在状态转换前，成为已接纳调用；要么排在转换后，立刻得到停止错误。不能先读状态、之后才进执行运行时或开始转发。

下列入口都用同一个 `admit`，没有各自的关闭判断：

- 公开 `Runtime.Invoke` 在归属和转发之前接纳。
- 入站 `invoke` handler 在交给本地执行运行时之前接纳。它不能直接调用内部执行运行时。
- 定时投递仍走根调用入口，因此也受同一规则约束。

探测不是实体调用，不登记调用数；但它读取同一根状态，在非 `running` 时拒绝回复。入站请求因停止被拒绝时，不先按另一套 `Done` 检查，也不因本地或转发来源而改变结果。

`closing`、`killing`，以及由这两条路径到达的 `stopped`，一律返回根包的 `ErrRuntimeClosed`，其稳定码为 `gor.runtime_closed`。`dead` 以及由它到达的 `stopped` 返回 `gor.node_dead`。这两个错误的跨节点重建规则见 `errors.md`；直接调用和转发调用按同一稳定码判断。内部的 mailbox、执行运行时或传输错误不能取代这条根层接纳结果。

### 优雅停止与突发停止

进入 `closing` 后，轮询器不再发起新调用，集群节点开始正常离开，mailbox 关闭。已接纳且已经在执行的方法可以完成；邮箱中尚未开始的调用按既有 mailbox 规则拒绝，不迁移也不重放。根运行时等待已接纳调用都 release、已执行的方法结束、停用完成，以及自己启动的基础设施 goroutine 退出，才 `finishStop`。传输必须留到已经接纳的转发请求和入站回复结束后再关闭。

进入 `killing` 后，同样先拒绝新调用，再取消已经执行的方法、拒绝 mailbox 队列、跳过尚未开始的停用钩子。它不等用户方法：Go 不能强制中止忽略取消的代码。`stopped` 在突发停止的运行时基础设施已经退出后到达，不表示这类用户代码一定已经返回。

`Close` 期间调用 `Kill` 必须立刻升级为后一条语义：取消已运行调用、拒绝队列并跳过未开始的停用钩子。所有正在等待 `Close` 完成的调用随这次升级按突发停止收束；不能继续以优雅停止的名义等待用户方法。内部执行运行时、集群节点和传输的关闭接口都必须支持这次升级。一个已经收到优雅停止命令的子组件不能把后到的 `Kill` 当作 no-op。

根运行时的等待只用 channel 表达：已接纳调用归零、执行运行时结束、集群节点结束、轮询器结束和传输结束各自关闭自己的完成 channel；停止协调者只接收这些 channel。短临界区可以保护一次状态转换或计数，但不得用 mutex、条件变量或 `WaitGroup` 等待另一次调用完成。

### 集群判死

集群节点必须报告自己结束的原因，不能只关闭一个无原因的 `Done` channel：主动 `Close` 也会把本节点成员行写成 `dead`，这不等于根运行时被别人判死。根运行时在仍为 `running` 时收到外部判死原因，调用 `becomeDead`，先关闭接纳和公开停止信号，再按突发停止收束。已经在 `closing` 或 `killing` 的根运行时保持原状态。

这样，内部执行运行时仍可在 `closing` 中排空，但它再也不是对外可达的「停止信号已关、仍能接纳调用」窗口。这个窗口现在是有名字、有入口规则和完成条件的状态，而不是 goroutine 执行顺序留下的空隙。

### 差距

根运行时的停止状态机已实装：`beginClose`、`beginKill`、`becomeDead`、`finishStop` 四个转换函数与原子的 `admit`/release 构成唯一接纳门，公开 `Runtime.Invoke`、入站 `invoke` handler 与定时投递共用同一个入口，在归属判断和转发之前接纳。`closing`/`killing` 与由这两条到达的 `stopped` 返回 `gor.runtime_closed`，`dead` 及由它到达的 `stopped` 返回 `gor.node_dead`。

停止协调已实装为纯 channel 等待：执行运行时暴露 `BeginClose`/`BeginKill` + `Done()` channel，集群节点暴露 `DeclaredDead()` channel，根协调者在 `closeGracefully`/`closeImmediately` 中只接收 `clusterDone`、`engine.Done()`、`drained`、`transportDone` 这些 channel。内部执行运行时支持 `closing → killing` 升级（`BeginKill` 从 `closing` 不是 no-op，而是关闭 `killing` channel、标记跳过未开始的停用钩子、取消执行）；集群节点通过 `DeclaredDead()` 显式报告「外部判死」而非「主动退出」，根层不再从「自己是否发起过停止」推断原因。判死节点不再发布最后的空视图，以免触发优雅迁移与突发停止竞起。

仍待实装的是：传输收尾必须晚于已接纳转发请求与入站回复关闭（当前 `closeTransport` 的相对顺序按本批已调整为在 `engine.Done` 与 `waitDrained` 之后，但根层未对接纳的转发请求单独跟踪其传输往返完成）。

当前 activation 不保存停用原因，`OnDeactivate` 只收到 `context.Background()`，错误出口传 `(Identity, method string, error)`。空闲、根关闭、所有权变化、panic 和丢弃都在停用汇合处丢失原因。上述停用原因、context 均尚未实装。

`Kill()` 的存在理由只有模拟测试——真实进程崩溃不会先礼貌地调一个函数。它不是给用户用的关机接口，用户要停机用 `Close()`。
