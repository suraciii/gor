# 设备影子示例

这是一个可以直接运行的设备影子服务：设备上报状态，服务保存最后一次上报；超过 30 秒没有新消息，设备会变成掉线，所属车间的在线数也会更新。

## 为什么这些东西是实体

示例中的 `Device` 和 `Workshop` 都有稳定身份，也各自负责一组必须串行更新的状态。

- **设备数量大，而且大多数时候闲置。** `Device` 用设备 id 作为身份，状态放在 `gor.State` 中；运行时可以把闲置激活从内存中驱逐，状态仍留在 store 里。`Device` 与 `Workshop` 的定义在 `domain/domain.go`，运行时装配在 `runtime.go`。
- **同一台设备的写入必须串行。** 上报和配置都直接改设备自己的影子，没有锁；同一身份的调用由 `gor` 排队执行。设备的方法仍然集中在 `domain/domain.go`，读者可以直接从 `Device` 接口看到这两个入口。
- **掉线是跟着设备走的一次性定时任务。** 每次上报都重置 `offline` 闹钟，任务触发后把设备改成掉线并通知车间；实现就在 `domain/domain.go` 的 `Report` 和 `MarkOffline`。
- **在线数是跨实体聚合。** 设备在上线、换车间和掉线时主动通知 `Workshop`；车间只保存在线设备的身份集合，不持有设备引用再逐个查询。`Device` 的通知调用和 `Workshop` 的集合状态都在 `domain/domain.go`。

这些不是为了把代码拆成更多类型，而是因为每个身份都需要独立激活、持有状态并接受串行调用。

## gor 不提供什么

设备影子写入、掉线闹钟重置和车间通知是三个独立操作。`gor` 不提供跨实体事务，也不会替用户补偿或回滚：如果影子写入成功、后续闹钟或车间通知失败，设备和车间可能暂时不一致。示例把错误返回给调用方，把这个窗口留给业务自己决定能否接受；实现顺序在 `domain/domain.go` 的 `Report` 方法中。

## 启动

在 gor 仓库根目录运行：

```bash
go run ./examples/shadow/cmd/shadow
```

服务监听 `:8080`，数据写入 `data/gor.db`。也可以指定地址和数据库文件：

```bash
go run ./examples/shadow/cmd/shadow -addr :9090 -db ./data/shadow.db
```

## 调用

让设备 `device-1` 上报到 `assembly` 车间：

```bash
curl -i -X POST http://localhost:8080/devices/device-1/reports \
  -H 'Content-Type: application/json' \
  -d '{"workshop_id":"assembly","state":"temperature=20"}'
```

成功返回 `204 No Content`。下发配置：

```bash
curl -i -X PUT http://localhost:8080/devices/device-1/configuration \
  -H 'Content-Type: application/json' \
  -d '{"configuration":"sample-rate=10s"}'
```

读取设备影子：

```bash
curl http://localhost:8080/devices/device-1/shadow
```

返回内容包含最后上报的状态、上报时间、在线状态、车间和配置，例如：

```json
{
  "reported_state": "temperature=20",
  "reported_at": "2026-08-04T12:00:00Z",
  "online": true,
  "workshop_id": "assembly",
  "configuration": "sample-rate=10s"
}
```

读取车间在线数：

```bash
curl http://localhost:8080/workshops/assembly/online-count
```

```json
{"online_count":1}
```

设备每次上报都会重置一个 30 秒的一次性闹钟。停止上报并等待 30 秒后，再读取影子会看到 `online` 变为 `false`，车间在线数变为 `0`。一次性闹钟触发后会从 schedule 表删除；设备再次上报时会重新上线并创建新的闹钟。

## 观察闲置驱逐

负载程序不是性能测试，不报告吞吐量或延迟。它使用临时 SQLite，并发让一批设备上报，再给 `device-000` 下配置；等待运行时驱逐闲置激活后重新读取它的影子：

```bash
go run ./examples/shadow/cmd/load
```

程序会确认 `device-000` 的配置仍然存在。这说明激活已经离开内存，状态留在 SQLite 里，下次调用时从 store 恢复。程序退出时会删除临时目录。也可以用 `-devices` 调整这批设备的规模。
