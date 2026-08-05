# research —— 证据基线

这一层是 [`design/`](../design/README.md) 里各项决策的**实测依据**。

与 `design/` 的区别：`design/` 写「我们要怎么做」，`research/` 写「我们看到了什么」。两者分开是为了让决策可以被重新审视——如果将来某个决策要翻案，先看它依赖的事实是否还成立。

## 记录纪律

- **只写实测到的东西。** 行数是数出来的，star 数是查出来的，源码结论是读出来的。推断与判断要标明。
- **带测量日期。** 生态数据会过期，过期的数据不删，标注日期让读者自己判断。
- **说清测量方法。** 「Orleans 有 27 万行」这种数字没有方法就没有意义。

## 篇目

- [orleans-internals.md](orleans-internals.md) —— Orleans 10.1 源码实测：规模构成、单激活仲裁、membership 无共识、调度器。
- [landscape.md](landscape.md) —— 同类方案现状：Go 侧的虚拟 actor 实现、durable execution 阵营、已死的先例。
- [go-capabilities.md](go-capabilities.md) —— Go 平台相对 .NET 的优势与劣势，以及每一条对设计的影响。
- [embedded-store-bench.md](embedded-store-bench.md) —— 嵌入式存储后端在统一条件下的读写与冷启动实测，是持久化选型可回看的证据。

## 测量时间

全部数据采集于 **2026-07-30**，对象为 Orleans 10.1（`dotnet/orleans` 稀疏 clone）与各项目当时的 GitHub 状态。
