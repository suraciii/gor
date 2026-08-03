# docs —— 产品 spec

这一层描述 **`gor` 该满足什么**：用户需求、API 面、心智模型、责任边界。

用产品语言和领域语言写，面向使用者，假设读者不读源码。实现术语（goroutine 调度、代码生成机制、存储表结构）归 [`design/`](../design/README.md)。

## 标注惯例

`docs/` 描述的是目标状态，不是现状。实现进度见 [../ROADMAP.md](../ROADMAP.md)——不在每篇文档里重复标注 WIP。

当某篇文档与实现出现显著差距时，在文内单列「差距」小节说明现状。**正文是 spec，差距是脚注。**

## 篇目

- [vision.md](vision.md) —— 定位、三条原则、非目标、与相邻方案的关系。
- [programming-model.md](programming-model.md) —— 编程模型与 API 形状。
