# Agent Instructions

## 项目状态

**第 1 到 4 步已实装。** 写代码前先读 [ROADMAP.md](ROADMAP.md) 确认要做的是哪一步，以及它的前置是否已完成。

不要为了「让仓库看起来有东西」写占位代码、半成品或假实现。

## 仓库布局

```
docs/       产品 spec —— gor 该满足什么（产品语言 + 领域语言）
design/     设计 spec —— 系统该怎么实现（可用技术语言）
research/   实测证据 —— 支撑 design 决策的事实
```

## Spec 先行

先把方案写进 `docs/` 或 `design/`，再实现。实装追赶 spec，不是 spec 跟着实装走。文档里出现尚未实装的能力是正常的。

某篇文档与代码有显著差距时，在文内单列「差距」小节说明现状。**正文是 spec，差距是脚注。**

`docs/` 禁止技术语言（包名、函数签名、存储表结构）。那些归 `design/`。

## 不可协商的约束

这几条来自 [design/testing.md](design/testing.md)，违反任何一条都会让确定性模拟测试整体失效——而 DST 是本项目的主要差异点：

1. 所有 I/O 在接口后面。
2. 时间通过注入的 `Clock` 获取。生产代码里出现 `time.Now()` 即视为 bug。
3. 组件是显式状态机，状态转换是可枚举的函数。
4. **跨调用的等待用 channel，不用 mutex。** mutex 阻塞在 `synctest` 里不算 durably blocking。这条连带禁用了 `x/sync/singleflight`。

ROADMAP 第 4 步（DST 骨架）必须在第 6 步（集群）之前。顺序不可协商——这四条约束无法事后加装。

## 测试

```bash
make test    # 单元测试，单个 < 50ms，不起网络不起进程
make sim     # 确定性模拟测试，慢，不进默认 test
make gen     # 生成器端到端测试，起 go list 子进程，不进默认 test
make lint    # vet + staticcheck
```

改完代码务必跑 `make test` 与 `make lint`。改动涉及 `runtime` / `cluster` / `store` 时还要跑 `make sim`。

禁止：真实外部依赖、真实时间（`time.Sleep` 做同步、轮询墙钟做断言）、`t.Skip` 掩盖偶发失败、新旧测试并存。

需要 Go 1.25+（`testing/synctest` GA 版本）。

## 注释原则

默认不写注释；用命名、类型、函数边界让代码自解释。只有当代码无法表达「为什么这样做」时才写——外部系统限制、关键不变量、反直觉选择。

注释在解释「做什么/怎么做」，就重构代码让注释消失。

注释不引用设计文档、issue 或任务编号——文档会改名移动，引用必然腐烂。历史归 git log。

## 设计原则

模型尽可能简洁，只包括必要的属性。

不加保护性特判——越死板，特例越多。
