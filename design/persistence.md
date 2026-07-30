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

## ETag 不是可选的

在集群模式下，目录是最终一致的——节点故障与网络分区期间，同一个 key 可能在两个节点上各有一个激活（详见 [cluster.md](cluster.md)）。此时唯一阻止状态互相覆盖的东西就是 ETag。

所以 ETag 不是「高级功能」，是**这个架构下正确性的必要条件**。API 上不提供绕过它的写入路径。

这与 Orleans 的立场一致：官方文档在讲到默认目录的最终一致性时，推荐的缓解手段正是存储层的 ETag 乐观并发。

## 后端

目标是两个：

**嵌入式** —— 单节点默认。候选 SQLite（`modernc.org/sqlite`，纯 Go，无 CGO）、bbolt、pebble。

倾向 SQLite：它同时能满足状态存储和协调表两个需求（协调表需要事务 + CAS，bbolt 的单写者模型也能做，但 SQL 表达 membership 那种多行条件更新更直接），而且运维可观测性最好——出问题能用 `sqlite3` 直接看。代价是比 bbolt/pebble 慢，纯 Go 版 SQLite 更慢。**选型在实现第 2 步时用实测数字定，现在不预先拍板。**

**Postgres** —— 集群模式。理由是它是唯一能同时满足「多节点共享」「支持条件更新做 CAS」「运维团队已经有」三条的选择。

不做 Redis 后端：membership 表需要的原子条件更新在 Redis 上要靠 Lua 脚本，而且 Redis 的持久化语义会让「死亡投票丢了」这类故障难以推理。

不做云厂商专有存储（DynamoDB / Azure Tables 那类）。Orleans 花在这上面的代码实测约 2.6 万行，是它体积的重要来源，收益对本项目不成立。

## 序列化实体状态

用户状态怎么变成 `[]byte`：默认 JSON。

理由不是 JSON 快，是**可读**——出问题时能直接看存储里是什么。性能敏感的用户可以注入自己的 `Codec`。

不做版本容忍序列化（字段增删自动兼容）。Orleans 为此付了 3 万行代码，`gor` 的立场是：状态结构演进由用户在应用层处理（读旧格式、写新格式），运行时不介入。

## 写入时机

`State.Set()` 立即写存储，同步等待完成。

否决了「批量延迟落盘」：它引入一个「内存已改、存储未改」的窗口，崩溃时丢数据，而且这个窗口会让 DST 里的不变量断言变得极其难写。性能不够就让用户减少 `Set` 次数（在一个方法里算完再写一次），而不是让运行时偷偷缓冲。

## 定时任务表

```
schedule(entity_type, entity_key, name, due_at, interval, etag)
```

轮询器扫 `due_at <= now`，取到的行先用 CAS 把 `due_at` 推到下一周期（这一步就是抢占所有权），成功了再投递调用。

这个顺序很关键：**先抢占再执行**。反过来会在崩溃时重复触发。而抢占成功但执行前崩溃，则会漏一次触发——这是有意的取舍，`gor` 承诺 at-most-once 的**投递**，不承诺 exactly-once 的**执行**。文档要写清。
