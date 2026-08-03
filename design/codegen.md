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

返回值个数不限。参数与返回值目前在进程内直传，不经过序列化；跨节点传输是第 6 步的事。

```go
type Account interface {
    Deposit(ctx context.Context, amount int64) (int64, error)
}
```

不满足契约的方法，生成器报错并指出行号——**不静默跳过**。静默跳过会让用户以为方法生成了，跑起来才发现没有。

这套契约来自 `alecthomas/go-rpcgen` 的做法（interface + 命名返回值 + 末位 error），是 Go 里被验证过的形状。

## 哪些接口会被生成

只生成带标记的：

```go
//gor:entity
type Account interface { ... }
```

包里的其他 interface 生成器不看。

不用「包里所有导出的 interface」这条规则：那样一个普通的辅助 interface 也要被拿去检查契约，用户为了让生成器闭嘴得把它挪到别的包去。标记显式，而且和「不满足契约就报错」这条能共存——没标记的不检查，标记了的必须合规。

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

## 多个返回值

`Invoke` 只有一个 `reply`，而方法可以返回多个值。所以**每个方法生成一个回复结构体**，代理和分发函数都用它：

```go
type accountDepositReply struct {
    R0 int64
}
```

不为「只有一个返回值」开特例。单值时直接传 `&reply` 确实少一层，但那样生成器就有两条路径，而两条路径要各自测、各自维护。生成的代码没人读，少一层缩进不值这个价。

只返回 `error` 的方法不生成回复结构体，代理传 `nil`。这不是给它开特例——没有值要装的时候本来就没有结构体可言，生成一个空的出来是死代码。

## 生成物怎么接进运行时

生成物落在子包，用户的接口包不引用它（原因见下面的类型检查死锁）。那么运行时怎么知道 `Account` 的分发函数在哪？

生成器额外产出一个安装函数：

```go
gorgen.Install(rt)
```

它把每个接口的分发函数和代理构造函数登记到 `rt` 上。之后：

```go
gor.Register[Account](rt, factory)        // 分发函数从登记表里取
acct := gor.Ref[Account](rt, "alice")     // 代理从登记表里取，返回值类型是 Account
```

`Register` 因此少一个参数——第 1 步手写的那个 `dispatch` 由生成器接管。这是计划内的破坏性改动。

**否决了 `init()` 自动登记。** 它能省掉 `Install(rt)` 这一行，代价是用户必须记得写一个空导入，忘了就是运行时才发现的失败，而且登记表变成进程级全局——第 4 步的模拟测试要在一个进程里跑多个节点。登记表挂在 `rt` 上，`Install` 显式调用，两个问题一起没有。

生成器不硬编码类型名字符串。它产出的是 `gor.InstallType[Account](rt, dispatch, newProxy)` 这样的泛型调用，名字由 `gor` 自己按跟 `Register` / `Ref` 同一套规则算——三处用同一个函数，就不存在算不到一起的可能。

登记表里没有的类型，`Register` 返回错误。`Ref` 没有 error 返回值，找不到就 panic。这不是运行期状况，是接线没接上：同一个类型的 `Register` 会在启动时先报出来，`Ref` 的 panic 只是兜底。

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

## 生成器怎么测

`go/packages` 会起 `go list` 子进程，一次几百毫秒。默认测试套件的规矩是单个测试 50 ms 以内、不起进程，所以生成器要拆成两层：

- **加载层**：`go/packages` + `go/types` → 一个纯数据的模型（接口名、方法名、参数类型串、返回值类型串、出错时的行号）。
- **渲染层**：模型 → 源码。

渲染层不认识 `go/types`，测试直接手搓模型，跑得飞快，进默认套件。加载层的端到端测试要真起工具链，单独一条 target。

这个拆分不只是为了测试。契约检查的报错发生在加载层，渲染层拿到的模型按定义已经合规——两层的职责本来就该分开。

## 调用方式

```bash
go run github.com/suraciii/gor/cmd/gorgen -pkg ./domain
```

或 `//go:generate`。

不做 `go build` 时自动生成——Go 没有这个钩子，硬做只能靠 wrapper 脚本，而那会让 `go test ./...` 这种最常用的命令行为变得不可预测。生成是显式一步，CI 里加一个「生成物与源码一致」的检查。

## 差距

**导入别名冲突。** 生成器按包名写导入，两个不同路径的包重名（`a/domain` 和 `b/domain`）会产出编译不过的代码。方法签名里出现跨包类型时才会碰到。生成器应当自己分配别名，现在没做。

## 否决的方案

**运行时 reflect 合成代理。** Go 的 `reflect.MakeFunc` 能构造函数值，但不能构造实现任意接口的类型。做不到。

**反过来让用户写 struct，生成 interface。** 少一次手写，但用户看不到自己的 API 面，而 API 面是给调用方看的最重要的东西。

**protobuf/IDL 先行。** 要求用户在 Go 之外再学一套语言，且 IDL 表达不了 Go 类型系统的全部。`gor` 的立场是 Go interface 就是 IDL。
