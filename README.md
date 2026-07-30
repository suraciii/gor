# gor

**Go 里的持久化有状态运行时。** 单二进制、库形态、可嵌入，从第一天就为确定性模拟测试而设计。

> **状态：设计阶段，无实现。** 本仓库当前只有设计文档与调研证据。`design/` 描述的是目标，不是现状。实现进度见 [ROADMAP.md](ROADMAP.md)。

## 这是什么

一个 Go 库，让你把「有身份、有状态、单线程执行、崩溃后能恢复」的对象作为编程单元。你写普通的 Go interface 和普通的 struct，`gor` 负责激活、串行化、持久化、定时唤醒，以及（可选的）跨节点分布。

思想来源是 Microsoft Orleans 的虚拟 actor 模型，但**这不是 Orleans 的移植**。取舍在 [adr 与 design 文档](design/README.md) 里逐条记录，最重要的三条：

- 编程模型是**编译期类型化**的，不是 `any` 进 `any` 出——通过从 Go interface 生成代理实现（[design/codegen.md](design/codegen.md)）。
- 单节点是**一等公民**，不是集群的退化模式。不需要 sidecar、不需要外部数据库，`import` 进去就能用。
- 确定性模拟测试是**架构约束**而非事后补的测试手段（[design/testing.md](design/testing.md)）。这是本项目相对同类实现的主要差异点。

## 为什么存在

Go 生态在这个位置有个空缺。把「有状态、崩溃透明、可嵌入」三个条件放在一起看：

| | 语言 | 形态 | 单节点可用 |
|---|---|---|---|
| Temporal | Go | 独立 server + worker | 需要 server 与数据库 |
| Restate / Rivet | Rust | 单二进制 server | 是，但不是 Go 库 |
| Dapr | Go | sidecar 进程 | 多一个部署单元 |
| goakt | Go | 库 | 是，但 API 是 `any`，且维护集中在单人 |

详细实测见 [research/landscape.md](research/landscape.md)。

## 不做什么

`gor` 明确不追求这些，理由见 [docs/vision.md](docs/vision.md)：

- 不做 Orleans 的 API 兼容层，也不追求概念一一对应。
- 不做通用 actor 框架（不提供监督树、mailbox 策略、行为切换这些 Akka 式能力）。
- 不做工作流 DSL 或编排图。
- 不追求「无限水平扩展」。目标规模是单机到小集群。

## 文档

- [docs/vision.md](docs/vision.md) —— 定位、原则、非目标。判断一个改动是否对齐方向时看这里。
- [docs/programming-model.md](docs/programming-model.md) —— 用户视角的编程模型与 API 形状。
- [design/](design/README.md) —— 架构、各子系统设计、技术取舍。
- [research/](research/README.md) —— 支撑上述决策的实测证据（Orleans 源码实测、生态现状、Go 侧能力边界）。
- [ROADMAP.md](ROADMAP.md) —— MVP 切分与验收标准。

## 开发

```bash
make test        # 单元测试
make sim         # 确定性模拟测试
make lint        # vet + staticcheck
```

需要 Go 1.25 或更高——`testing/synctest` 在 1.25 才 GA，而它是测试策略的地基。

## 语言

当前文档为中文。若将来公开发布，英文化是发布前的必办项之一，见 ROADMAP。

## License

MIT，见 [LICENSE](LICENSE)。
