# design —— 设计 spec

这一层描述 **系统该怎么实现**：架构边界、数据模型、接口、技术选型与取舍。

可以用技术语言。与 [`docs/`](../docs/README.md) 的分工：`docs/` 说该满足什么，`design/` 说怎么做到。

## 标注惯例

正文是 spec，不是现状说明。[ROADMAP.md](../ROADMAP.md) 说到哪一步了，实装就到哪里；后面几步的内容在这里已经写好，读的时候别当成代码里有的东西。

某篇与代码有显著差距时，篇内单列「差距」小节。

## 篇目

- [architecture.md](architecture.md) —— 包边界、依赖方向、什么放哪里。
- [runtime.md](runtime.md) —— 激活、目录、生命周期。
- [scheduling.md](scheduling.md) —— 串行执行、重入、mailbox。
- [persistence.md](persistence.md) —— 状态存储、CAS、后端选型。
- [timers.md](timers.md) —— 持久化定时任务：表、轮询器、投递语义。
- [cluster.md](cluster.md) —— membership、放置、目录一致性。
- [codegen.md](codegen.md) —— 从 Go interface 生成类型化代理。
- [testing.md](testing.md) —— 单元测试与确定性模拟测试。
- [simulation.md](simulation.md) —— 模拟测试骨架：种子、故障注入、崩溃、事件日志。

## 决策记录

重要取舍直接写在对应篇目里，不另建 ADR 目录。每条取舍要写清「否决了什么」和「代价是什么」——只写结论的取舍等于没记录。
