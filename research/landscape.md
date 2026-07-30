# 生态现状

测量日期 2026-07-30。star 数会变，日期在这里是为了让读者判断数据是否还新鲜。

## 市场共识已经从「虚拟 actor」漂移到「持久化执行」

| 项目 | stars | 语言 | 形态 |
|---|---|---|---|
| Temporal | 21,947 | Go | 独立 server + 数据库 + worker |
| Rivet | 5,762 | Rust | 单二进制 server |
| Restate | 4,233 | Rust | 单二进制 server |

Restate 的 "virtual objects" 就是 grain 换了个名字：有 key 的对象 + 持久 K/V + 串行执行。它把 Orleans 的模型重新包装成 durable execution 卖出去了。

这是 `gor` 定位跟着 durable execution 走的原因（[../docs/vision.md](../docs/vision.md)）：卖点是「崩溃后接着跑」，不是「actor 模型」。用 actor 的词汇讲同一件事，市场已经不接了。

## Go 侧的虚拟 actor 实现

**goakt**（`Tochemey/goakt`）—— Go 里最接近的库。实测它的 grain API：

```go
// actor/grain.go
type Grain interface {
    OnActivate(ctx context.Context, props *GrainProps) error
    OnReceive(ctx *GrainContext)
    OnDeactivate(ctx context.Context, props *GrainProps) error
}

// actor/grain_of.go:65
func GrainOf[T Grain](ctx context.Context, system ActorSystem, name string, opts ...GrainOption) (*GrainIdentity, error)

// actor/actor_system.go:582
AskGrain(ctx context.Context, identity *GrainIdentity, message any, timeout time.Duration) (response any, err error)
```

两处关键观察：

1. **`any` 进 `any` 出。** `GrainContext.Message() any`（grain_context.go:162）、`Response(resp any)`（:258）。所有类型错误推迟到运行时。这是 `gor` 做代码生成的直接动因（[../design/codegen.md](../design/codegen.md)）。
2. **没有持久化状态。** 通读 `actor/grain_option.go`，没有 `WithGrainStateStore` 或任何状态存储选项——**不存在 `[PersistentState]` 的对等物**。用户得自己接存储。

另外 `GrainFactory` 已标 Deprecated，转向 `GrainOf` + `WithGrainDependencies`——API 还在动。

维护集中在单人。这对一个要被放进生产系统的基础库是实打实的可信度问题。

**proto.actor**（Go 版）—— 是 Akka 式 actor 加上 "virtual actor"（cluster grain）。它的 grain 走 protobuf IDL 生成，类型是有的，但代价是引入一套 IDL 和 protobuf 依赖，而且核心仍然是 Akka 式监督模型，不是持久化对象模型。

**Dapr** —— 有虚拟 actor，Go 写的。但形态是 sidecar：多一个部署单元、多一跳网络、多一套配置。不是库。

## 已死的先例

**Orbit**（`orbit/orbit`）—— EA / BioWare 做的 JVM 虚拟 actor 实现，Orleans 启发。

- 1,724 stars
- 用 Kotlin 重写过一轮
- **2021-06-15 后停更**
- `orbit-legacy/Orbit1` 只有 9 stars

技术上它没输给谁。它死于生态——没有形成用户群，公司内部需求变了，就没人推了。

这是 [../ROADMAP.md](../ROADMAP.md) 风险小节里「生态风险」的实证：这个位置上有过一个资源充足、技术过关的项目，然后它死了。所以差异化定位（单二进制库形态 + DST + 类型安全）比功能对齐 Orleans 更重要。

## DST 工具链

`gor` 的测试策略依赖确定性模拟测试，所以专门查了 Go 侧有什么可用：

| 工具 | 状态 | 能不能用 |
|---|---|---|
| `testing/synctest` | **Go 1.25 GA** | 能，但只覆盖单元测试层 |
| gosim（`jellevandenhooff/gosim`） | 80 stars，**2024-12 后停更** | 形状最对（多机 + 确定性 goroutine 调度），但不能依赖一个停更的地基 |
| Antithesis | 商业产品 | 能用，2025-09 报价 **16.8 万美元/年** |
| porcupine（`anishathalye/porcupine`） | 1,230 stars，活跃 | 能用，但它只检查历史是否可线性化，不控制调度 |

Resonate 团队的公开结论与此一致：Go 里无法控制 goroutine 调度，DST 只能靠侵入式地约束整个代码库。

**结论**：没有可依赖的现成 DST 框架，`sim` 包必须自建，而自建的前提是代码库从第一天就满足那几条约束。这就是为什么 DST 在 `gor` 里是架构约束而不是测试任务（[../design/testing.md](../design/testing.md)）。

## 跨语言移植的成功案例长什么样

tsgo（TypeScript 编译器的 Go 移植）是这类项目里最成功的一个。它成功的两个条件值得记录，因为它们**对 Orleans 都不成立**：

1. **有确定性的预言机** —— TypeScript 有一整套 conformance 测试，输入输出都是确定的文本，移植后逐个对比就知道对不对。Orleans 的正确性藏在 258 个 `TestCluster` 测试和 136 个 `Sleep` 里，没有这样的预言机。
2. **纪律是 port 而不是 rewrite** —— 逐文件、逐函数对照，不重新设计。而 Orleans 的模型本身在漂移（创始人已走、Orleans 10 在转 durable execution），照搬 2014 年的设计是移植一个正在被作者放弃的目标。

所以 `gor` 明确**不是 port**。这个判断是本仓库存在形态的根据：重新设计，只继承那个仍然成立的核心想法（用 key 引用、运行时负责激活、按 key 串行）。
