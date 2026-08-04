# 设备影子示例

这是一个可以直接运行的设备影子服务：设备上报状态，服务保存最后一次上报；超过 30 秒没有新消息，设备会变成掉线，所属车间的在线数也会更新。

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
