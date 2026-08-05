# 编程模型

> 本文描述目标 API。实现进度见 [../ROADMAP.md](../ROADMAP.md)。

## 核心概念

只有三个。

**实体（Entity）** —— 有身份、有状态的对象。你写一个 Go struct 加一组方法。运行时保证同一个身份上的调用串行执行。

**身份（Identity）** —— 类型 + key。`Account("alice")` 和 `Account("bob")` 是两个不同的实体，`Account("alice")` 永远指向同一个。不需要创建，也不需要销毁：第一次调用它就存在了，闲置够久了它就从内存里消失，状态留在存储里，下次调用再回来。

**调用（Call）** —— 通过接口调方法。调用方不知道也不关心目标在本进程还是在别的节点上。

## 声明一个实体

先写接口：

```go
//gor:entity
type Account interface {
    Deposit(ctx context.Context, amount int64) (int64, error)
    Balance(ctx context.Context) (int64, error)
}
```

接口方法的第一个参数必须是 `context.Context`，最后一个返回值必须是 `error`。中间的参数和返回值随便写几个。不合规矩的方法在生成时报错，指出行号。

### 生成的前置步骤

`//gor:entity` 标记表示这个接口要生成类型化调用。新建或修改带标记的接口后，构建前要显式运行生成；可以交给 `go generate`，也可以单独执行。命令、生成文件的位置和检查方式见 [../design/codegen.md](../design/codegen.md)。

每个运行时启动时都要安装这次生成的结果，随后才能注册实体或取得实体引用。下面的启动示例给出了安装的位置。

再写实现：

```go
type account struct {
    balance gor.State[int64]
}

func (a *account) Deposit(ctx context.Context, amount int64) (int64, error) {
    if amount <= 0 {
        return 0, errors.New("amount must be positive")
    }
    v := a.balance.Get() + amount
    if err := a.balance.Set(ctx, v); err != nil {
        return 0, err
    }
    return v, nil
}

func (a *account) Balance(ctx context.Context) (int64, error) {
    return a.balance.Get(), nil
}
```

注册：

```go
gor.Register[Account](rt, func(b *gor.Binder) Account {
    return &account{balance: gor.NewState[int64](b, "balance")}
})
```

`b` 是运行时递进来的，用来把状态格子接到存储上。除此之外工厂就是一个普通的构造函数。

方法体里没有锁，因为不需要——同一个 key 上不会有第二个调用同时在跑。

## 实体知道自己是谁

```go
type account struct {
    id      gor.Identity
    balance gor.State[int64]
}

gor.Register[Account](rt, func(b *gor.Binder) Account {
    return &account{
        id:      gor.Self(b),
        balance: gor.NewState[int64](b, "balance"),
    }
})
```

要打日志、要把 key 当业务数据用（`Account("alice")` 里的 `alice` 就是用户名）、要调另一个实体并告诉它自己是谁，都需要这个。

**身份不是状态。** 它不进存储，不会因为实体被驱逐又重新激活而变，也不会因为写冲突而回滚。同一个身份在两个节点上同时激活时，两份激活的 `id` 也是同一个值。

## 实体读时间

`Binder` 只在激活时交给工厂一次。方法体里要用到它，就在工厂里把它留下来：

```go
type device struct {
    b       *gor.Binder
    reading gor.State[reading]
}

gor.Register[Device](rt, func(b *gor.Binder) Device {
    return &device{b: b, reading: gor.NewState[reading](b, "reading")}
})

func (d *device) Report(ctx context.Context, value float64) error {
    next := d.reading.Get()
    next.ReportedAt = gor.Now(d.b)
    ...
}
```

不要用 `time.Now()`。实体读到的时间必须来自运行时——测试要控制它，模拟测试里每个节点的时钟还可以带不同的偏移。这跟库自己的规矩是同一条。

## 实体调另一个实体

跟从外面调是同一个函数，换一个第一参数：

```go
gor.Ref[Workshop](d.b, workshopID).DeviceOnline(ctx, deviceID)
```

外面拿运行时，里面拿 `Binder`。**实体不需要为了调别人去捕获运行时对象**——工厂的签名就是 `func(b *gor.Binder) T`，那一个参数够用。

跨实体调用是虚拟实体最常做的事。它得跟本地方法调用一样顺手，否则用户会把逻辑堆进一个巨大的实体里来躲开它。

## 调用一个实体

```go
acct := gor.Ref[Account](rt, "alice")
balance, err := acct.Deposit(ctx, 100)
```

`acct` 的类型是 `Account`。写错参数类型、调不存在的方法，都是编译错误。这是与 `any`-based API 的关键区别，代价是需要跑一次代码生成，见 [../design/codegen.md](../design/codegen.md)。

### 集群调用与部署的限制

调用的写法不变，但集群里有三件事要知道：

**方法返回的错误只剩下文本。** `errors.Is` 和 `errors.As` 认不出跨节点回来的错误——错误的类型过不了网络。要让调用方能分辨的情况，编进正常的返回值里，别指望错误类型。

**取消传不进方法体。** 本地调用时 `ctx` 取消，方法体里的 `ctx` 也就取消了；跨节点时只有你这边不再等待，对面的方法会跑完。

参数和返回值跨节点时要过一遍 JSON，所以它们必须是 JSON 编得动的类型。本地调用不过这一遍——同一个方法本地跑得通、跨节点炸掉，来源通常就是这里。

**同一集群不能靠不停机滚动升级来承载不兼容的改动。** 集群里的节点要使用彼此兼容的应用版本。方法签名或状态格式不兼容时，停机发布还是应用自己安排双写，由应用决定。

## 调用的结局与顺序

同一个实体会排队处理调用。队列满时，新调用会直接因过载被拒绝，方法没有开始执行，状态不会因此改变。

超时或取消只表示调用方不再等结果。方法可能已经开始，甚至已经改了状态；跨节点调用遇到发送后的网络错误也是这样，调用方不能从这个错误判断方法有没有执行。不要把这两类结局当成过载拒绝来重试。

方法 panic 会让这次调用返回错误，当前实例随即被丢弃。已经排队但还没开始的调用也会以错误结束，不会交给新实例重跑。下一次调用会从持久状态重新建立实例。

实体处理调用时不会接着处理第二个调用。A 调 B、B 又调回 A 这样的调用环会失败，不能靠等待解开。运行时不自动重试：是否可以重试、怎样避免重复业务动作，只能由调用方判断。

同一调用方在本地连续发给同一实体的调用，按发起顺序执行。跨节点不保证这个顺序；有先后依赖的操作要由业务数据表达依赖，不能依赖网络到达顺序。

## 状态

`gor.State[T]` 是状态的载体。`Get()` 读内存里的当前值，`Set()` 写并持久化。

一个实体可以有多个格子，名字用来区分它们。它们一起存成一条记录，所以任何一个格子写入都会更新整个实体的版本。

**格子里放 map 或 slice 时，`Get()` 拿到的就是那一份，不是复制品。** 改完必须 `Set()` 回去才算数——只改不写，值在内存里变了，存储里没变，实体被驱逐再回来就退回旧值。要不要先复制一份再改是风格问题，落盘与否只看有没有 `Set()`。

每次 `Set()` 都会立即尝试持久化。成功后才成为当前持久值；失败时保留上一次确认的值，并丢弃当前实例。错误返回后不要再假定这个实例可继续使用，下一次调用会重新读回状态。

一个方法里的多次 `Set()` 不是一次事务。前面的写可能已经成功，后面的写仍可能失败；需要原子业务结果时，要由业务自己把相关数据组织成一次状态更新。

状态必须能用 JSON 编码。运行时不替应用兼容状态结构的演进；字段增删或格式变更由应用负责读旧格式、写新格式。

**并发语义要说清楚**：在集群模式下，运行时**不保证**同一时刻全世界只有一个 `Account("alice")` 在跑。节点故障与网络分区期间存在双激活窗口。因此 `Set()` 带乐观并发检查，冲突时返回错误而不是静默覆盖。

这不是实现偷懒——Orleans 的默认目录也是这个语义，而且官方文档就是这么写的（见 [../research/orleans-internals.md](../research/orleans-internals.md)）。单节点模式下不存在这个窗口。

## 定时唤醒

状态用 `gor.State[T]` 接到存储上，定时任务同样从 `b` 拿一个格子：

```go
type account struct {
    balance  gor.State[int64]
    schedule gor.Schedule
}

func (a *account) Open(ctx context.Context) error {
    return a.schedule.Set(ctx, "monthly-interest", gor.Every(30*24*time.Hour), "ApplyInterest")
}

func (a *account) ApplyInterest(ctx context.Context) error { ... }
```

定时任务是持久的：进程崩溃后到期仍会触发。到期时对象不在内存里，就把它唤醒。

到期打的是**方法名**，不是函数值——崩溃之后没人能把一个闭包恢复回来，能存下来的只有名字。被打的方法只收 `ctx`、只回 `error`。

它不是 `time.AfterFunc`：别指望毫秒级精度，也别指望停机期间错过的次数会补回来（回来只打一次，然后接着往下走）。

同名的任务一个对象上只有一份，再 `Set` 一次是改期。

任务可以是一次性的，也可以周期触发；取消后不再保留。一次性任务到期后只投递一次，然后消失。

定时唤醒承诺的是**至多一次投递**，不是方法恰好执行一次。系统会先确认这次到期已被取走，再投递方法；若在两者之间崩溃，这次触发可能漏掉。方法失败后也不会自动重试，错误仍走下面的错误出口。

状态变更与设置、改期或取消定时任务不是一个原子业务操作。它们可能只有一边成功；需要一起成立的业务语义，要由应用处理这个窗口。

## 生命周期钩子

```go
OnActivate(ctx context.Context) error     // 从存储恢复状态之后、处理第一个调用之前
OnDeactivate(ctx context.Context) error   // 驱逐之前，最后的落盘机会
```

两个都是可选的：实体上有这个方法就会被调用，没有就不调。

`OnActivate` 返回错误，这次激活就不成立——触发它的那个调用拿到这个错误，下一个调用会重新试一次。

`OnDeactivate` 返回错误改变不了任何事，实体照样消失。这时候已经没有调用方在等了，错误会送到运行时的错误出口（见下）。

## 没人在等的错误

有两件事发生时，没有调用方能收到错误：定时任务到期后方法失败了，和 `OnDeactivate` 失败了。

默认这些错误被丢掉。要看见它们，给运行时配一个出口：

```go
gor.New(gor.OnError(func(id gor.Identity, method string, err error) {
    log.Printf("%s.%s: %v", id, method, err)
}))
```

运行时只负责把错误送到这里。要不要重试、要不要告警，是你的事。

**这是运行时唯一一处替你吞掉错误的地方。** 别的错误都从方法的返回值回来。

## 运行时观测

运行时交给应用两类事实。第一类是本节点当前激活的快照：哪些实体正在服务，以及每个实体有多少已排队、尚未开始的调用。它只观察本节点，不替集群汇总。

第二类是每次调用完成的事件。事件给出调用方看见的结果、耗时和目标的类型与方法。调用方取消时，事件照样记录取消这个结果；即使方法后来继续完成，也不会再有第二个事件。跨节点调用只在始发节点记录一次，不在接收节点重复记录。

完成事件的回调与调用方同步执行。回调不能阻塞或做 I/O，否则延迟的就是调用方自己的结果。运行时不做监控数据的聚合、导出或告警，这些由应用接到已有系统。

## 运行时启动

单节点，状态落在本地文件：

```go
database, err := store.OpenSQLite("data/gor.db")
if err != nil { return err }
defer database.Close()

rt, err := gor.New(gor.WithStore(database))
if err != nil { return err }
defer rt.Close()

gorgen.Install(rt)
```

`Install` 把生成的代理和分发函数交给运行时。少这一行，`Register` 和 `Ref` 会在启动时报错——不会拖到第一次调用。

集群要明确交给运行时状态存储、共享成员表、本节点地址、这次启动的 generation 和传输：

```go
nodeTransport, err := transport.New(":7373")
if err != nil { return err }

rt, err := gor.New(
    gor.WithStore(stateStore),
    gor.WithMemberStore(memberStore),
    gor.WithNodeAddr(nodeTransport.Addr()),
    gor.WithGeneration(generation),
    gor.WithTransport(nodeTransport),
)
if err != nil {
    nodeTransport.Close()
    return err
}
defer rt.Close()
```

`memberStore` 由所有节点共享；`generation` 在同一地址的每次重新加入时都要换新值。`Runtime.Close` 会关闭配置的传输。单节点到集群的差别是配置，不是业务代码。

## 运行时可能自己停下来

集群里，一个节点可能被别人判死。判死之后它不再服务任何实体——继续用一个全世界都认为已经死了的身份提供服务，只会写出别人看不见的数据。

所以运行时给一个信号：

```go
<-rt.Done()   // 关掉了，或者被判死了
```

它 close 就意味着这个运行时不再工作。你的进程该退出，或者建一个新的运行时重新加入。

不看这个信号也不会静默出错——之后的每次调用都会返回错误。但那时候你的服务已经在对外装作正常了。

## 心智模型对照

如果你用过其他系统：

| 概念 | Orleans | Temporal | Restate | gor |
|---|---|---|---|---|
| 有身份的状态对象 | Grain | —— | Virtual Object | Entity |
| 身份 | GrainId | WorkflowId | Object Key | Identity |
| 持久状态 | `[PersistentState]` | Workflow 变量 | 内建 K/V | `State[T]` |
| 定时唤醒 | Reminder | Timer | —— | Schedule |

对照表只帮你建立直觉。语义不完全等价，尤其是 Temporal 的 workflow 有确定性重放约束，`gor` 没有——`gor` 靠持久化状态而不是重放事件日志来恢复。
