# 错误与取消

## 目标

调用结果可以离开进程。任意 Go `error` 对象不能。跨节点结果因此只投影为稳定码和文本，不尝试把对象装回去。

投影规则只看错误是否带 `Code`。它不按错误的具体类型列白名单。框架错误在产生处自己带码；应用错误由应用自己带码；其余错误一律是不透明错误。

## 公开类型

```go
type Code string

type Coded interface {
	Code() Code
}

func CodeOf(error) (Code, bool)
```

`Code` 实现 `error` 和 `Coded`。应用用包级常量声明它：

```go
const ErrWorkshopIDRequired gor.Code = "shadow.workshop_id_required"
```

`CodeOf` 从错误本身和单值 `Unwrap() error` 链中找第一个 `Coded`。它不走 `Unwrap() []error`。因此 `errors.Join` 不跨节点保真；除非最外层错误自己带码，join 结果按不透明错误处理。

远端重建的带码错误也实现 `Coded`。它的 `Is` 只比较 `Code`。这使下列断言在本地和远端都成立：

```go
errors.Is(err, ErrWorkshopIDRequired)
```

这是 `errors.Is` 的完整对等范围。远端错误不等于原错误，不恢复任意 sentinel，不保证 `errors.As` 得到原类型，也不保留包装层数、字段或 join 成员。

## 码空间

有效码形如 `<owner>.<name>`。两段都由小写 ASCII 字母、数字和下划线组成，且以字母开头。`owner` 是码的归属边界。应用选择自己拥有的 `owner`；`gor` 保留。

本版框架码集合封闭如下：

| Code | 结局 |
| --- | --- |
| `gor.no_owner` | 当前视图没有可路由的拥有者。 |
| `gor.node_dead` | 目标节点已停止服务。 |
| `gor.runtime_closed` | 运行时或实体 mailbox 已关闭。 |
| `gor.overloaded` | 调用在方法开始前因队列满被拒绝。 |
| `gor.type_not_installed` | 目标节点没有该实体类型。 |
| `gor.unknown_method` | 目标类型不含所请求的方法。 |
| `gor.invalid_request` | 请求形状或参数不能按当前契约解码。 |
| `gor.persistence_conflict` | 状态写入遇到版本冲突。 |
| `gor.persistence_failed` | 状态写入失败，且不是版本冲突。 |
| `gor.panic` | 工厂或实体方法 panic。 |
| `gor.request_encode_failed` | 源端不能把参数编码为调用请求。 |
| `gor.reply_encode_failed` | 成功调用的返回值不能编码。 |
| `gor.transport_failed` | 请求、响应或连接传递失败；执行结局未知。 |

框架不得为同一结局在这组码之外另造 `gor.*` 码。应用不得使用 `gor.*`。本版不会为任意错误类型注册额外映射，也不会从错误文本推导码。

## 调用响应信封

调用响应改为：

```go
type errorEnvelope struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message"`
}

type callResponse struct {
	Reply json.RawMessage `json:"reply,omitempty"`
	Error *errorEnvelope `json:"error,omitempty"`
}
```

`Error == nil` 表示调用成功。非 nil 时 `Code` 可为空，`Message` 必填。空码表示不透明错误；接收端重建一个只提供文本的错误。非空码重建一个 `Coded` 错误，令 `errors.Is` 按码匹配。

调用处理按这个顺序执行：

1. 执行方法，先得到业务错误。
2. 业务错误非 nil 时，只投影它到 `Error`，不编码 `Reply`。
3. 业务错误为 nil 时，编码 `Reply`。编码失败则返回带 `gor.reply_encode_failed` 的 `Error`。
4. 最后编码整个 `callResponse`。

第 2 步是优先级规则。回复编码是诊断层故障，不能替换已经得到的业务错误。第 4 步失败时没有可发送的调用结果，按 `gor.transport_failed` 处理；它不能覆盖一个已经可发送的业务错误。

请求编码失败和 `Send`、响应解码失败也以 `gor.request_encode_failed` 或 `gor.transport_failed` 返回。`gor.transport_failed` 不表示请求未送达、方法未开始或状态未改变。

## 本地与远端

本地调用不经过信封。它保留原错误对象。只要错误链中有声明的 `Code`，本地 `errors.Is` 已按 Go 标准规则匹配该码。

远端调用在服务端投影错误、在源端重建错误。投影使用 `CodeOf`，重建使用只按码匹配的错误。因此已声明码是两种位置共享的唯一错误身份。文本可以补充上下文，但不得影响任何分支。

框架在所有公开调用路径上构造表中的码，包括本地路径。这样 `errors.Is` 对框架码也不依赖调用位置。内部包的旧 sentinel 可以继续作为内部实现细节，但不得作为 `gor` 公开调用结果的唯一身份。

## 取消

`Runtime.invoke` 把调用方 `ctx` 传给 `transport.Send`。`Send` 因该 `ctx` 完成时，源端直接返回 `ctx.Err()` 并移除本地 pending 请求。

接收端 handler 的 context 来自服务生命周期，不派生自发送方 `ctx`，也不带发送方 deadline。没有取消帧和 deadline 字段。请求一旦被接收，远端方法只会因远端自身的关闭、终止或方法逻辑而取消。

所以源端取消有三条要求：

1. 源端返回原始 `ctx.Err()`，不等待远端。
2. 远端上下文不取消，远端方法继续执行。
3. 后到的远端结果被丢弃，不写回调用方的 reply。

调用方没有可观察的“已经送达”边界。它不能从取消、超时或 `gor.transport_failed` 推出远端未执行。传输失败不能证明未执行的规则独立于取消规则，必须保留。

## 不做的事

不增加任意类型注册、字段序列化、错误链或 `errors.Join` 保真、错误码 codegen 注解、取消帧或远端 deadline 传播。它们都扩大 wire contract，却不改变稳定码这一条唯一的跨节点错误身份。

## 差距

当前 `callResponse.Error` 是 `string`，服务端在编码 `Reply` 失败时覆盖已有 `Error`，客户端以 `errors.New` 重建文本。现有 `Code`、信封投影、统一框架码、取消规则的显式测试和迁移均待实现。
