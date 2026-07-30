# 代码生成

## 要解决什么

Go 泛型不能凭 `T` 合成一个实现 `T` 的类型。所以

```go
acct := gor.Ref[Account](rt, "alice")   // 返回 Account
```

没法纯靠泛型实现。同类 Go 项目的通行解法是把类型丢掉：

```go
resp, err := system.AskGrain(ctx, identity, msg, timeout)   // any 进 any 出
```

goakt 就是这样（实测 `AskGrain(ctx, *GrainIdentity, message any, timeout) (any, error)`，`GrainContext.Message() any` / `Response(any)`）。代价是所有类型错误推迟到运行时。

`gor` 不接受这个代价，走代码生成。

## 输入契约

生成器读用户写的 Go interface。合法的实体接口方法必须：

- 第一个参数是 `context.Context`
- 最后一个返回值是 `error`
- 中间参数与返回值可编码

```go
type Account interface {
    Deposit(ctx context.Context, amount int64) (int64, error)
}
```

不满足契约的方法，生成器报错并指出行号——**不静默跳过**。静默跳过会让用户以为方法生成了，跑起来才发现没有。

这套契约来自 `alecthomas/go-rpcgen` 的做法（interface + 命名返回值 + 末位 error），是 Go 里被验证过的形状。

## 输出

每个接口生成一个代理：

```go
type accountProxy struct {
    id gor.Identity
    rt gor.Invoker
}

func (p *accountProxy) Deposit(ctx context.Context, amount int64) (int64, error) {
    var reply int64
    err := p.rt.Invoke(ctx, p.id, "Deposit", []any{amount}, &reply)
    return reply, err
}
```

以及一个服务端侧的分发函数，把方法名 + 参数还原成对实现类型的直接调用。

## 关键解耦

生成物只依赖一个窄接口：

```go
type Invoker interface {
    Invoke(ctx context.Context, id Identity, method string, args []any, reply any) error
}
```

这一条决定了生成代码的稳定性：**运行时内部怎么改都不需要重新生成。** 传输换了、目录换了、序列化换了，生成物不动。

这个技巧来自 `segmentio/glue`——用一个窄 `Call` 接口让生成物与传输实现彻底解耦。

## 实现路径

用 `golang.org/x/tools/go/packages` 加载包，`go/types` 拿类型信息，`text/template` 出代码。

**已知的坑**：`go/types` 要求被加载的包能通过类型检查。如果生成物和用户接口在同一个包，那么「生成物还不存在」→「用户代码引用它 → 包类型检查失败」→「生成器无法加载包」，死锁。

解法：**生成物落在子包**（例如 `internal/gorgen/`）。用户接口所在的包不引用生成物，由 `gor.Register` / `gor.Ref` 在运行时通过注册表关联。

## 调用方式

```bash
go run github.com/suraciii/gor/cmd/gorgen -pkg ./domain
```

或 `//go:generate`。

不做 `go build` 时自动生成——Go 没有这个钩子，硬做只能靠 wrapper 脚本，而那会让 `go test ./...` 这种最常用的命令行为变得不可预测。生成是显式一步，CI 里加一个「生成物与源码一致」的检查。

## 否决的方案

**运行时 reflect 合成代理。** Go 的 `reflect.MakeFunc` 能构造函数值，但不能构造实现任意接口的类型。做不到。

**反过来让用户写 struct，生成 interface。** 少一次手写，但用户看不到自己的 API 面，而 API 面是给调用方看的最重要的东西。

**protobuf/IDL 先行。** 要求用户在 Go 之外再学一套语言，且 IDL 表达不了 Go 类型系统的全部。`gor` 的立场是 Go interface 就是 IDL。
