# 确定性约束检查

## 目的

把可以由语法树精确判断的确定性测试约束接入 `make lint`。检查器只检查源码事实，不尝试从调用图或控制流推断「所有 I/O 在接口后面」和「组件是显式状态机」；这两条继续由人工评审负责。

## 判据

检查器遍历仓库中的 Go 源文件，跳过 Git 元数据和 vendored 外部源码。文件路径、行号和列号随每条诊断输出。

生产代码中，导入 `time` 后直接使用以下函数均违规：

- `Now`
- `Since`
- `Until`
- `After`
- `Tick`
- `NewTimer`
- `NewTicker`
- `Sleep`

唯一例外是 `clock/clock.go` 中 `clock.Real` 的 `Now` 和 `NewTicker` 实现；它们是注入时钟的真实实现，必须直接连接标准库。例外按文件、接收者和方法名同时匹配，不放宽整个 `clock` 包。

测试代码中，`time.Sleep` 和 `t.Skip`、`t.Skipf`、`t.SkipNow` 均违规。测试应使用 channel、`testing/synctest` 或明确的失败断言表达同步和失败。

任何代码导入 `golang.org/x/sync/singleflight` 均违规，因为它用 mutex 等待，不能在 `testing/synctest` 中表达 durable blocking。

检查器支持默认导入名、别名导入和 dot import，既检查直接调用，也检查函数值使用，避免通过改名或保存函数值绕过规则。检查器使用 `go/build/constraint` 解析 `//go:build` 和 `// +build`，不根据文件名猜测构建目标。

## sim 夹具边界

默认库源码中的 `time.Sleep` 没有例外，`time.Sleep(0)` 也违规。只有 `sim/` 下、构建约束明确要求 `sim`、且文件不是 `_test.go` 的模拟夹具可以使用 `time.Sleep`。这里的 `Sleep` 表示注入的存储响应延迟，由 `testing/synctest` bubble 统一调度；共享 fake store 不属于某个节点，把它的延迟绑到节点 `Clock` 没有清晰语义，未来节点时钟有偏移时尤其如此。

这个边界按构建目标和职责划分，不是按现有四个调用点加特判，也不是给 `sim` 包开时间 API 白名单：sim 夹具中的 `time.Now`、`time.After` 等仍违规，sim 标签的 `_test.go` 仍禁止 `time.Sleep`，避免它成为测试同步手段。
