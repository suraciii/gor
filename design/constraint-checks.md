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

唯一例外是 `clock/clock.go` 中 `clock.Real` 的 `Now` 和 `NewTicker` 实现；它们是注入时钟的真实实现，必须直接连接标准库。例外按文件、接收者和方法名同时匹配，不放宽整个 `clock` 包。

测试代码中，`time.Sleep` 和 `t.Skip`、`t.Skipf`、`t.SkipNow` 均违规。测试应使用 channel、`testing/synctest` 或明确的失败断言表达同步和失败。

任何代码导入 `golang.org/x/sync/singleflight` 均违规，因为它用 mutex 等待，不能在 `testing/synctest` 中表达 durable blocking。

检查器支持默认导入名、别名导入和 dot import，避免通过改名绕过规则。

## 当前差距

`clock.Clock` 目前没有 `Sleep` 或等价的假时钟等待接口。模拟存储因此在 `sim/store.go` 使用 `time.Sleep` 模拟 bubble 内的故障延迟；该行为由 [design/simulation.md](simulation.md) 明确规定。检查器暂不把生产代码的 `time.Sleep` 列为可执行规则，避免在没有替代接口的情况下制造无法修复的 CI 红灯；这项 API/模拟设计取舍需要另行决定。
