# Go 平台能力边界

把 Orleans 的做法搬到 Go，哪些变简单、哪些变难。每条都标出对设计的影响。

## Go 更省的地方

**没有 async/await 的颜色问题。** Orleans 需要 823 行自定义 `TaskScheduler`（`WorkItemGroup` 336 行 + `ActivationTaskScheduler` 171 行 + ……），目的是保证 `await` 之后回到同一逻辑执行上下文。Go 里 goroutine 本身就是执行上下文，同样语义约 100 行——一个 goroutine 从一个 channel 读，循环执行。

→ [../design/scheduling.md](../design/scheduling.md)

**不需要序列化子系统。** Orleans 的序列化实测 30,761 行，主要复杂度在版本容忍（滚动升级时新旧节点互通）。`gor` 放弃这个能力，用标准库能覆盖的编码就够。

→ [../design/architecture.md](../design/architecture.md)（含代价说明：不支持不兼容变更的不停机升级）

**单二进制分发。** 这不只是打包方便。它决定了产品形态：不需要装 server、不需要跑 sidecar、`import` 进去就能用。Temporal / Restate / Rivet / Dapr 都做不到这一点，这是 `gor` 存在的主要理由。

**嵌入式存储可选项多且都是纯 Go。** `modernc.org/sqlite`、bbolt、pebble 都无需 CGO，单节点持久化不引入外部依赖。

→ [../design/persistence.md](../design/persistence.md)

**`testing/synctest`（Go 1.25 GA）。** 假时钟 + 「durably blocked」静止判定，直接消灭了「`time.Sleep` 然后祈祷」这个最大的 flaky 来源。.NET 侧没有等价物——Orleans 的 136 个含 `Sleep` 的测试文件就是这个缺口的证据。

## Go 更难的地方

**没有 `AsyncLocal`。** .NET 里 `RequestContext` 靠 `AsyncLocal` 隐式沿异步调用链传播。Go 里只能显式塞进 `context.Context` 并层层传递。

影响：调用环检测（需要沿链传「已占用的实体」集合）必须显式穿透所有中间层。这是实打实的劣势，没有优雅解法。

→ [../design/runtime.md](../design/runtime.md)

**反射能力弱。** `reflect.MakeFunc` 能构造函数值，但**不能构造实现任意接口的类型**。所以「给我一个 `Account`，调用转发到远端」这件事无法在运行时完成。

影响：必须做代码生成。这是 `gor` 相对 goakt 多出来的一个构建步骤，换来编译期类型安全。

→ [../design/codegen.md](../design/codegen.md)

**没有版本容忍序列化。** 上面算作「省了 3 万行」的同一件事，从另一面看是能力缺失。Orleans 能不停机滚动升级到不兼容的方法签名，`gor` 不能。

**没有可用的 DST 框架。** gosim 停更、Antithesis 每年 16.8 万美元、porcupine 只检查历史不控制调度。Go 运行时不提供 goroutine 调度控制。

影响：`sim` 包必须自建，而且它对生产代码提出四条硬约束（I/O 在接口后、时间可注入、组件是显式状态机、等待用 channel 不用 mutex）。这是全项目最大的持续性成本。

→ [../design/testing.md](../design/testing.md)

## 两个容易踩的具体坑

**mutex 阻塞不算 durably blocking。** 在 `synctest` bubble 里，goroutine 阻塞在 `sync.Mutex` 上不会被判为静止，`synctest.Wait()` 会挂或误判。

后果：任何用 mutex 做「等待」的实现都会让测试策略失效。所以连 `golang.org/x/sync/singleflight` 都不能用（它内部是 mutex），激活去重要自己用 channel 实现。

**`go/types` 要求包能通过类型检查。** 如果生成物和用户接口同包，会形成死锁：生成物不存在 → 用户代码引用它 → 包类型检查失败 → 生成器加载不了包 → 生成不出来。

解法：生成物落子包，用户包不直接引用生成物。

## Go 侧可参考的代码生成先例

| 项目 | 可借鉴的点 |
|---|---|
| `alecthomas/go-rpcgen` | 输入契约的形状：interface + 命名返回值 + 末位 error |
| `segmentio/glue` | 用一个窄 `Call` 接口让生成物与传输实现解耦 |
| Encore | metadata 驱动生成，而非纯语法树驱动 |

三者合起来给出了 [../design/codegen.md](../design/codegen.md) 的设计：从 Go interface 读契约，生成物只依赖窄 `Invoker` 接口，运行时内部变更不需要重新生成。
