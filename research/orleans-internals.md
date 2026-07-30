# Orleans 源码实测

对象：Orleans 10.1，`dotnet/orleans` clone 后直接读源码。测量日期 2026-07-30。

## 规模构成

`src/` 共 **27.4 万行 / 1897 文件**。但这个数字会误导，拆开看：

| 部分 | 行数 | 对 gor 的意义 |
|---|---|---|
| `src/api/` API 基线快照 | 35,384 | **不是实现代码**，是 API 审批用的签名快照 |
| 序列化 | 30,761 | 不做（不追求版本容忍） |
| 流 / 事务 / 事件溯源 / journaling | 35,247 | 不在范围内 |
| 云厂商 provider | ~26,000 | 换成嵌入式 + Postgres 两个后端 |
| **真正需要重建的核心** | **~26,000** | 见下 |

核心的构成：

| 子系统 | 行数 |
|---|---|
| GrainDirectory | 7,136 |
| Catalog（激活生命周期） | 5,817 |
| MembershipService | 5,811 |
| Placement | 5,123 |
| ConsistentRing | 884 |
| Scheduler | 823 |

**结论：要写的是 2 到 3 万行，不是 24 万行。** 而且 Go 侧还能再省（见 [go-capabilities.md](go-capabilities.md)）。

## 单激活仲裁只有约 25 行

`src/Orleans.Runtime/GrainDirectory/GrainDirectoryPartition.Interface.cs` 的 `RegisterCore`——整个「保证同一个 grain 只有一个激活」的仲裁逻辑：

```csharp
private GrainAddress RegisterCore(GrainAddress newAddress, GrainAddress? existingAddress, MembershipVersion currentVersion)
{
    ref var existing = ref CollectionsMarshal.GetValueRefOrAddDefault(_directory, newAddress.GrainId, out _);
    if (existing is null || existing.Matches(existingAddress) || IsSiloDead(existing))
    {
        if (newAddress.MembershipVersion != currentVersion)
            newAddress = new() { ... MembershipVersion = currentVersion };
        existing = newAddress;
    }
    return existing;   // 输者拿回权威地址，去那边转发
}

private bool IsSiloDead(GrainAddress existing)
    => existing.SiloAddress is null
    || _owner.ClusterMembershipSnapshot.GetSiloStatus(existing.SiloAddress, existing.MembershipVersion) == SiloStatus.Dead;
```

对一个内存 dict 做 CAS，三个条件之一成立就写入：没人占、占的正是我预期的那个、占的那个节点已经死了。返回值总是权威地址——赢了拿到自己的，输了拿到对方的然后转发过去。

调用点在 `DhtGrainLocator.cs:37`：`_localGrainDirectory.RegisterAsync(address, currentRegistration: previousAddress)`。

**这个发现改变了对项目难度的判断。** 最让人望而生畏的那个保证，实现是单线程 dict 上的一次条件写。

## 单激活是明确的弱保证

Orleans 官方文档明说：默认 grain directory 是**最终一致的**，集群不稳定期间**允许出现重复激活**。推荐的缓解手段是存储层的 ETag 乐观并发。

这不是文档的免责声明，是设计立场。`gor` 采纳同样立场并在用户可见处写明（[../design/cluster.md](../design/cluster.md)、[../design/persistence.md](../design/persistence.md)）。

## Membership 不含共识

在 `src/Orleans.Runtime/MembershipService/` 下 grep `quorum|Quorum|consensus|Consensus|Raft|Paxos`：**零命中**。

它的做法是：

- 一张共享表，用 ETag/CAS 做原子更新（`MembershipTableManager.cs`）。
- 节点互相探测（`ProbingSiloHealthMonitor.cs:47,93`）。
- 探测失败投「死亡票」，**票会过期**——`ClusterHealthMonitor.cs:195` 的 `entry.GetFreshVotes(now, options.DeathVoteExpirationTimeout)` 只统计新鲜票。
- 节点还监控自身健康（`LocalSiloHealthMonitor.cs:168-176`），自己不健康时不该有资格判别人死。

线性一致性完全外包给存储。这意味着 `gor` 也不需要实现共识——需要的是一个支持条件更新的表。

## 调度器 823 行，Go 里不需要

`src/Orleans.Runtime/Scheduler/` 全部文件：

| 文件 | 行数 |
|---|---|
| WorkItemGroup.cs | 336 |
| ActivationTaskScheduler.cs | 171 |
| ClosureWorkItem.cs | 151 |
| SchedulerExtensions.cs | 89 |
| TaskSchedulerUtils.cs | 41 |
| WorkItemBase.cs | 22 |
| IWorkItem.cs | 13 |
| **合计** | **823** |

`WorkItemGroup : IThreadPoolWorkItem, IWorkItemScheduler` 内部是 `lock (_lockObj)` 加一个队列。

这 823 行存在的原因是 .NET 需要自定义 `TaskScheduler` 来保证 `await` 之后回到同一逻辑上下文。Go 里 goroutine 本身就是执行上下文，同样语义约 100 行（[../design/scheduling.md](../design/scheduling.md)）。

## 测试套件不能当预言机

测试 **17.7 万行 / 1168 文件**。其中：

- **258 个文件**依赖 `TestCluster`（进程内起真实集群）
- **136 个文件**含 `Task.Delay` 或 `Thread.Sleep`

正确性藏在时序里。这条观察直接导出了 `gor` 的测试策略（[../design/testing.md](../design/testing.md)）：不可能移植这套测试，也不该模仿它。

## Orleans 自己的方向

Orleans 10 新增：

- `Orleans.Journaling` —— 8,148 行
- `Orleans.DurableJobs` —— 5,278 行，即 "Reminders v2"，**仍是 preview/alpha**

两个都指向持久化执行，不指向 actor 模型。`DurableJobs` 的存在说明 Orleans 自己也认为 Reminders v1 的设计需要换掉——这是 [../design/scheduling.md](../design/scheduling.md) 里选「表 + 轮询」而非复刻 v1 的依据。

创始人 Sergey Bykov（领导 Orleans 十余年）现在在 **Temporal Cloud 做架构**，并公开写过不再使用 "actors" 一词。
