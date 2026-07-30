# 编程模型

> 本文描述目标 API。**尚无实现**——下面的代码是设计意图，不是可运行的示例。

## 核心概念

只有三个。

**实体（Entity）** —— 有身份、有状态的对象。你写一个 Go struct 加一组方法。运行时保证同一个身份上的调用串行执行。

**身份（Identity）** —— 类型 + key。`Account("alice")` 和 `Account("bob")` 是两个不同的实体，`Account("alice")` 永远指向同一个。不需要创建，也不需要销毁：第一次调用它就存在了，闲置够久了它就从内存里消失，状态留在存储里，下次调用再回来。

**调用（Call）** —— 通过接口调方法。调用方不知道也不关心目标在本进程还是在别的节点上。

## 声明一个实体

先写接口：

```go
type Account interface {
    Deposit(ctx context.Context, amount int64) (int64, error)
    Balance(ctx context.Context) (int64, error)
}
```

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

## 调用一个实体

```go
acct := gor.Ref[Account](rt, "alice")
balance, err := acct.Deposit(ctx, 100)
```

`acct` 的类型是 `Account`。写错参数类型、调不存在的方法，都是编译错误。这是与 `any`-based API 的关键区别，代价是需要跑一次代码生成，见 [../design/codegen.md](../design/codegen.md)。

## 状态

`gor.State[T]` 是状态的载体。`Get()` 读内存里的当前值，`Set()` 写并持久化。

一个实体可以有多个格子，名字用来区分它们。它们一起存成一条记录，所以任何一个格子写入都会更新整个实体的版本。

**并发语义要说清楚**：在集群模式下，运行时**不保证**同一时刻全世界只有一个 `Account("alice")` 在跑。节点故障与网络分区期间存在双激活窗口。因此 `Set()` 带乐观并发检查，冲突时返回错误而不是静默覆盖。

这不是实现偷懒——Orleans 的默认目录也是这个语义，而且官方文档就是这么写的（见 [../research/orleans-internals.md](../research/orleans-internals.md)）。单节点模式下不存在这个窗口。

## 定时唤醒

```go
func (a *account) OnActivate(ctx context.Context) error {
    return gor.Schedule(ctx, "monthly-interest", gor.Monthly, a.applyInterest)
}
```

定时任务是持久的：进程崩溃后到期仍会触发。它不是 `time.AfterFunc`，别指望毫秒级精度。

## 生命周期钩子

```go
OnActivate(ctx context.Context) error     // 从存储恢复状态之后、处理第一个调用之前
OnDeactivate(ctx context.Context) error   // 驱逐之前，最后的落盘机会
```

两个都是可选的。

## 运行时启动

单节点，状态落在本地文件：

```go
rt, err := gor.New(gor.WithStore(gor.SQLite("data/gor.db")))
if err != nil { return err }
defer rt.Close()
```

集群，多加一行：

```go
rt, err := gor.New(
    gor.WithStore(gor.Postgres(dsn)),
    gor.WithCluster(gor.Membership(dsn), gor.Listen(":7373")),
)
```

单节点到集群的差别是配置，不是代码。业务代码一行不改。

## 心智模型对照

如果你用过其他系统：

| 概念 | Orleans | Temporal | Restate | gor |
|---|---|---|---|---|
| 有身份的状态对象 | Grain | —— | Virtual Object | Entity |
| 身份 | GrainId | WorkflowId | Object Key | Identity |
| 持久状态 | `[PersistentState]` | Workflow 变量 | 内建 K/V | `State[T]` |
| 定时唤醒 | Reminder | Timer | —— | Schedule |

对照表只帮你建立直觉。语义不完全等价，尤其是 Temporal 的 workflow 有确定性重放约束，`gor` 没有——`gor` 靠持久化状态而不是重放事件日志来恢复。
