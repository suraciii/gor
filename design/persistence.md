# 持久化

## 两个职责

`store` 包干两件不同的事，别混：

1. **实体状态** —— 按 Identity 读写一份状态，带乐观并发。
2. **集群协调表** —— membership 与定时任务需要的共享表，要求支持 CAS。

第 2 项是 `cluster` 正确性的地基（见 [cluster.md](cluster.md)），第 1 项只服务业务。

## 接口

```go
type Store interface {
    Read(ctx context.Context, id Identity) (Record, error)
    Write(ctx context.Context, id Identity, data []byte, expect ETag) (ETag, error)
}

type Record struct {
    Data []byte
    ETag ETag
}
```

`Write` 在 `expect` 与存储中当前 ETag 不符时返回 `ErrConflict`。**不做重试，不做合并。** 冲突是业务语义问题，运行时无权替用户决定。

空记录（首次访问）用零值 ETag 表示，`Write` 时传零值 ETag 意为「要求这条记录当前不存在」。

**记录不存在不是错误。** `Read` 读一条没有的记录，返回零值 `Record` 和 `nil` 错误。「不存在」和「存在但是空的」在这里是同一件事——实体第一次被激活时状态本来就是零值，没必要让每个调用点都去分辨这两种情况。

## ETag 不是可选的

在集群模式下，目录是最终一致的——节点故障与网络分区期间，同一个 key 可能在两个节点上各有一个激活（详见 [cluster.md](cluster.md)）。此时唯一阻止状态互相覆盖的东西就是 ETag。

所以 ETag 不是「高级功能」，是**这个架构下正确性的必要条件**。API 上不提供绕过它的写入路径。

这与 Orleans 的立场一致：官方文档在讲到默认目录的最终一致性时，推荐的缓解手段正是存储层的 ETag 乐观并发。

## 后端

目标是两个：

**嵌入式** —— 单节点默认。候选 SQLite（`modernc.org/sqlite`，纯 Go，无 CGO）、bbolt、pebble。

倾向 SQLite：它同时能满足状态存储和协调表两个需求（协调表需要事务 + CAS，bbolt 的单写者模型也能做，但 SQL 表达 membership 那种多行条件更新更直接），而且运维可观测性最好——出问题能用 `sqlite3` 直接看。代价是比 bbolt/pebble 慢，纯 Go 版 SQLite 更慢。**选型在实现第 2 步时用实测数字定，现在不预先拍板。**

要测的就是 `Store` 这两个方法在真实访问形态下的样子，不是通用读写吞吐：

- 按 key 点读一条记录。
- CAS 写一条记录（读改写，带 ETag 校验），单条即提交——因为 `Set()` 不批量。
- 多个 key 并发写。这条最能分开三个候选：bbolt 是单写者，pebble 和 SQLite 不是。
- 冷启动打开一个已有几万条记录的库要多久。

记录时要带上条件：记录大小、并发度、机器、是否开 WAL。**没有条件的数字不算数字。**

**Postgres** —— 集群模式。理由是它是唯一能同时满足「多节点共享」「支持条件更新做 CAS」「运维团队已经有」三条的选择。

不做 Redis 后端：membership 表需要的原子条件更新在 Redis 上要靠 Lua 脚本，而且 Redis 的持久化语义会让「死亡投票丢了」这类故障难以推理。

不做云厂商专有存储（DynamoDB / Azure Tables 那类）。Orleans 花在这上面的代码实测约 2.6 万行，是它体积的重要来源，收益对本项目不成立。

## State 怎么跟运行时接上

`gor.State[T]` 要知道自己属于哪个 Identity、写哪个 store、当前 ETag 是多少。用户写的 struct 里它只是一个字段，工厂函数 `func() Account { return &account{} }` 没有地方把这些交给它。

解法是工厂多收一个参数：

```go
gor.Register[Account](rt, func(b *gor.Binder) Account {
    return &account{balance: gor.NewState[int64](b, "balance")}
})
```

`NewState` 把这个格子登记到 binder 上，运行时激活实体时按登记表读一次 store、把值分发到各个格子。

**否决了反射扫结构体字段回填。** 它能让工厂保持 `func() Account`，用户少写一行，但代价是要用 `unsafe` 去写未导出字段，而且用户看不出这个字段是怎么活过来的。少写的那一行不值这个价。

## runtime 不导入 store

`runtime` 和 `store` 在架构图里是兄弟，谁也不导入谁。但 `Binder` 要同时够到两边：Identity 只有 `runtime` 在激活时才知道，`Store` 是 `gor` 组装配置时注入的。

解法是工厂由 `runtime` 调用、由 `gor` 提供：

```go
type Registration struct {
    Factory  func(context.Context, Identity) (any, error)
    Dispatch Dispatch
}
```

`runtime` 把 Identity 交出去，拿回一个不透明的实例。它不知道实例里挂着一个 Binder，也不知道构造过程读了一次存储。`gor` 是唯一同时看得见 `runtime` 和 `store` 的包，两个 Identity 类型的转换就发生在这一处。

工厂现在能返回错误——激活时读存储可能失败。这跟工厂 panic 合并成同一条路径：激活没建成，错误返回给调用方。

## 冲突怎么传回运行时

`Set()` 撞 `ErrConflict` 时要停用激活，但 `runtime` 不认识 `ErrConflict`，也不该认识。

不能靠错误往上冒。用户的方法完全可以吞掉这个错误返回 nil，而那时缓存的 ETag 已经过期了。停用必须与用户代码怎么处理错误无关。

所以 `Set()` 立刻在 Binder 上打一个标记，`gor` 的分发包装在每次调用结束后检查它，把结果包成 `runtime` 认得的形状：

```go
type Discard struct{ Err error }
```

`runtime` 看到 `Discard` 就停用这个激活，并把 `Err` 原样返回给调用方。它只知道「实体说自己不能再用了」，不知道为什么。这跟 panic 后停用走同一条路径。

## 一个实体一条记录

`Store.Read` 按 Identity 返回一条记录，所以一个实体的所有 `State` 格子合起来编码成一条记录：一个 JSON 对象，key 是 `NewState` 时给的名字。

这就是名字存在的理由——只有一个格子时它确实是多余的，但有第二个格子时就必须有东西区分它们，而为「只有一个格子」开一条免名字的特例只会让两种形态在存储里长得不一样。

任何一个格子 `Set()` 都重写整条记录。于是 **ETag 是实体级的**，不是字段级的——这正是双激活防护需要的粒度：两个激活改的哪怕是不同字段，也必须有一个撞冲突。

## 序列化实体状态

用户状态怎么变成 `[]byte`：JSON，不可替换。

理由不是 JSON 快，是**可读**——出问题时能直接看存储里是什么。

否决了让用户注入 `Codec`。一个实体的状态是多个格子合成的一条记录，外层容器和格子里的值必须用同一套编码。做成可插拔只有两种落法：外层永远是 JSON、只有格子走 codec——那 codec 是假的，非 JSON 的字节塞进 JSON 容器不成立；或者外层是 `map[string][]byte`——那 JSON 会把每个值 base64 掉，可读性没了，而可读性正是选 JSON 的全部理由。为一个还没人要的旋钮付这个价不值。

节点间传输的编码是另一件事，见 [architecture.md](architecture.md)，第 6 步再定。

不做版本容忍序列化（字段增删自动兼容）。Orleans 为此付了 3 万行代码，`gor` 的立场是：状态结构演进由用户在应用层处理（读旧格式、写新格式），运行时不介入。

## 写失败之后

`Set()` 的写返回任何错误时，做两件事：

1. **内存里的值不变。** 写没确认成功，格子就不能装作写成功了，否则内存和存储从此对不上，而且用户读到的是一个存储里可能根本不存在的值。
2. **停用这个激活。** 下次调用重新从 store 读，拿到新的 ETag 再继续。

这跟 panic 的处理是同一条思路（[runtime.md](runtime.md)）：实例的内存状态已经不可信了，就别接着用它，重建比修复便宜。

**不区分冲突和其他写错误。** 撞 `ErrConflict` 说明 ETag 确定已经过期；其他错误说明这次写的结局不明——存储可能已经改了，只是回复没送到。两种情况下这个激活手里的 ETag 都不可信，处理方式就该一样。分开处理要多一条规则，换来的只是「有时候还能凑合再用一会儿」。

错误照常返回给调用方，运行时不替他重试——重试是不是安全，只有他知道。

## 写入时机

`State.Set()` 立即写存储，同步等待完成。

否决了「批量延迟落盘」：它引入一个「内存已改、存储未改」的窗口，崩溃时丢数据，而且这个窗口会让 DST 里的不变量断言变得极其难写。性能不够就让用户减少 `Set` 次数（在一个方法里算完再写一次），而不是让运行时偷偷缓冲。

## 定时任务表

```
schedule(entity_type, entity_key, name, due_at, interval, etag)
```

这张表不走 `Store` 接口——扫描到期行、CAS 抢占、删行都塞不进「按 Identity 读写一份状态」里。它自己一个接口，细节见 [timers.md](timers.md)。
