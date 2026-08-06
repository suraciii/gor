# Agent Instructions

## 项目状态

实现进度以 [ROADMAP.md](ROADMAP.md) 为准，不要从本文件或代码自行推断。

不要为了「让仓库看起来有东西」写占位代码、半成品或假实现。

## 环境

Go 在 `$HOME/sdk/go1.26.5/bin`，staticcheck 在 `$HOME/go/bin`。两个目录都不在默认 PATH 里。开始工作先执行：

```bash
export PATH="$HOME/sdk/go1.26.5/bin:$HOME/go/bin:$PATH"
export GOPROXY=https://goproxy.cn
```

Go module proxy 必须走 `goproxy.cn`。`proxy.golang.org` 在这台机器上不通。

`/tmp` 是 tmpfs。任何 fsync 相关的测量放在 `/tmp` 里都是空转，数字作废。存储 benchmark 要把 `GOR_BENCH_DIR` 指到真盘。

## 仓库布局

```
docs/       产品 spec —— gor 该满足什么（产品语言 + 领域语言）
design/     设计 spec —— 系统该怎么实现（可用技术语言）
research/   实测证据 —— 支撑 design 决策的事实
```

## 协作

每个 agent 在自己的 worktree 里干活。按批次推进，每批做完停下来报告，等评审通过再进下一批。

依赖安装（`go get` / `go mod tidy`）是 agent 的事，不要指望别人代劳。不要让两个 agent 同时写 `go.mod`。

评审意见和代码不一致时，直接说：「你说的和代码对不上」。不要把代码改成评审意见描述的样子。

提交前只检查和提交自己负责的文件。不要 push，不要开 PR。

进度以 [ROADMAP.md](ROADMAP.md) 为准，它是叙事 spec：讲「做什么、为什么」，是大步骤和它们的理由。GitHub 的 milestone + issue 是执行单元：讲「这件具体的事、现在什么状态、归哪个版本」。issue 不复述设计，只指向 ROADMAP 或 design 文档的对应小节。

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
make net     # 真 TCP 的传输测试，不进默认 test
make lint    # vet + staticcheck
```

改完代码务必跑 `make ci`。

改动涉及 `runtime` / `cluster` / `store` 时，迭代过程中就单独跑 `make sim`，不要等到最后。

禁止：真实外部依赖、真实时间（`time.Sleep` 做同步、轮询墙钟做断言）、`t.Skip` 掩盖偶发失败、新旧测试并存。

需要 Go 1.25+（`testing/synctest` GA 版本）。

## 注释原则

默认不写注释；用命名、类型、函数边界让代码自解释。只有当代码无法表达「为什么这样做」时才写——外部系统限制、关键不变量、反直觉选择。

注释在解释「做什么/怎么做」，就重构代码让注释消失。

注释不引用设计文档、issue 或任务编号——文档会改名移动，引用必然腐烂。历史归 git log。

公开 API 的 doc comment 是使用者契约，不属于本节对实现注释的限制；它只写可依赖的行为，不叙述实现。

## 设计原则

模型尽可能简洁，只包括必要的属性。

不加保护性特判——越死板，特例越多。
