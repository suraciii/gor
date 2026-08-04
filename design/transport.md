# 传输

节点之间搬字节。**这一层不懂消息的语义**——它不知道什么是 Identity、什么是方法、什么是实体。

## 不用 gRPC

需要的功能只有一件：把一段字节送到对面，拿回一段字节。

gRPC 附带的 protobuf 契约、HTTP/2 栈、连接管理策略，都是要被绕过或者对抗的东西。而 DST 要求传输完全在接口后面、能被内存假实现替换——自己写一层薄的更容易做到，也更容易看懂。

## 接口

```go
type Handler func(ctx context.Context, payload []byte) ([]byte, error)

type Transport interface {
    Send(ctx context.Context, addr string, payload []byte) ([]byte, error)
    Serve(ctx context.Context, h Handler) error
    Addr() string
    Close() error
}
```

地址是一个字符串，不是 `cluster` 里的节点类型。传输不导入 `cluster`——依赖是反着的。

**绑定和服务分开。** 监听地址在构造时就绑好，`Addr()` 立刻能返回实际绑定的地址，`Serve` 只是接受循环。节点要先知道自己的地址才能往成员表里写自己那一行，而测试用 `:0` 让内核挑端口——不分开就拿不到。

## 一个方向一条连接

A 发给 B 的请求走 A 拨给 B 的那条连接，B 的回复也从那条回来。B 发给 A 的请求走 B 拨给 A 的另一条。

两个节点之间因此有两条 TCP 连接，不是一条。代价明确，换掉的是「一条入向连接能不能拿来发出向请求」这类仲裁逻辑——那种逻辑只在两边同时拨号的竞态里才走到，是典型的写十行、错三年。

## 帧

```
[4 字节 payload 长度][8 字节关联 id][1 字节类型][payload]
```

大端。类型三种：请求、响应、错误。

**长度有上限，超过就断开连接。** 不是保护性特判——没有上限时，一个错位的长度前缀会让对面按几个 GB 去分配内存。上限是常量，不做成配置。

**错误只搬文本，不搬类型。** 错误帧的 payload 就是错误字符串。需要结构化错误的，自己编进正常响应的 payload 里，在上一层解。传输认得错误类型就等于认得语义。

## 多路复用

一条连接上同时飞着多个请求，靠关联 id 对上号。

**pending 表只有一个 goroutine 碰。** 每条连接三个 goroutine：读、写、owner。owner 持有 pending 表和下一个关联 id，它的输入全是 channel——新请求、读到的响应帧、请求被取消、连接死了。

不用 mutex 保护 pending 表。这不是洁癖：`synctest` 里阻塞在 mutex 上不算 durably blocking，一个 mutex 就能让 bubble 判不出静止。同样的理由在 `runtime` 的激活占位那里已经出现过一次，这是第二次。

`Send` 把请求连同一个回复 channel 交给 owner，然后 select 回复和 `ctx.Done()`。

**服务端的 handler 各自一个 goroutine，不许在 owner 里跑。** owner 只做登记和转交，跑完 handler 再回来是把这条连接上所有别的请求堵在后面——关联 id 存在的全部理由就是不让它们互相等。上一层尤其经不起这个：`gor` 把多个实体的调用塞进同一条连接，而实体调用本来就是串行的，一个实体正忙就会拖住来自同一个节点的所有其他实体。

handler 写回响应也走 channel 交回 owner，帧仍然只有 owner 一个人排。连接要死的时候取消 handler 的 ctx 并等它们退出——留一个还在跑的 handler，`Close()` 就成了赌运气。

## 结局不明

`Send` 的 ctx 到期时，请求可能已经在对面执行完了。

取消只做一件事：告诉 owner 把这条 pending 丢掉。之后真到了的响应找不到登记，直接扔掉。

**调用方拿到的错误不代表对面没执行。** 这跟 `runtime` 那边超时的语义是同一条，也跟「写失败但生效了」是同一类事。上一层不许把传输错误当成「没发生」。

## 连接死了就是死了

懒拨号：第一次发给某个地址时才建连接。

连接出错时，owner 让所有 pending 以错误返回，连接关掉。**不做重连循环、不做退避、不做保活探测。** 下一次 `Send` 发现没有连接，重新拨一次；拨不通就报错。

判定一个节点是不是真的没了，是成员表的事（[cluster.md](cluster.md)）。传输在这件事上有意见只会造成两套互相矛盾的判断。

## 编码不在这一层

传输搬的是不透明字节。编码发生在 `gor` 那一层，用 `encoding/json`——跟实体状态落盘同一套，不引入第二套序列化故事。

选它不是因为快，是因为它已经在项目里了，而且线上内容人能直接读。[architecture.md](architecture.md) 说明了为什么不自研格式，以及放弃了什么。

## 测试

帧和多路复用全部用 `net.Pipe` 测——内存里的双向管道，不起网络，进 `make test`。乱序响应、超时后迟到的响应、超长帧、连接中途断开，都在这里穷举。

真 TCP 只测拨号和监听这一小块，单独一条 `make net`，跟 `make gen` 一样不进默认 test。

带故障注入的假传输——分区、重排、丢弃——属于模拟测试，见 [simulation.md](simulation.md)。它实现的是同一个 `Transport` 接口，不是同一份代码。

## 差距

`transport` 包尚未实装。上面是它的 spec。
