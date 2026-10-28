# Python 远程 Actor Counter

[English](README.md) | 中文

独立 Python provider 注册 `demo.counter`，consumer 通过 HTTP 发送增量并获得累计值，Go gateway 借助内部 actorx-dist/NATS 按需创建代理。provider 和 consumer 只需要 gateway URL 与同一个 API Key，不监听端口、不安装 NATS 客户端。

## 运行

需要 Go 1.27、Python 3.11+ 和支持 JetStream 的 `nats-server`。以下命令均从 actorx-dist 仓库根目录执行。

```sh
python3 -m venv /tmp/actorx-remote-demo-venv
/tmp/actorx-remote-demo-venv/bin/python -m pip install -e remote/sdk/python
```

用 `secrets.token_urlsafe(32)` 生成随机 API Key，通过受信环境为 gateway、provider、consumer 三个终端注入相同的 `REMOTE_API_KEY`。Key 不作为命令行参数；gateway 缺少 Key 时拒绝启动。

终端 1，启动内部 NATS，使用独立的开发存储目录：

```sh
nats-server -c examples/nats.conf -sd /tmp/actorx-remote-counter
```

终端 2，启动 gateway：

```sh
GOWORK=off go run ./remote/examples/counter/gateway \
  -nats nats://127.0.0.1:4222 -listen 127.0.0.1:8080
```

终端 3，注册 provider：

```sh
/tmp/actorx-remote-demo-venv/bin/python remote/examples/counter/provider.py \
  --server http://127.0.0.1:8080 --type demo.counter
```

终端 4，调用同一个 Addr 两次，再调用另一个 ID：

```sh
/tmp/actorx-remote-demo-venv/bin/python remote/examples/counter/consumer.py \
  --server http://127.0.0.1:8080 --type demo.counter --id orders-42 --count 2
```

consumer 完成后自行退出。按 provider → gateway → NATS 的顺序 Ctrl-C 关闭其余进程。再次运行同一 ID 的 consumer 时，已有实例的计数会继续递增。

## 预期输出

首次运行 consumer 输出三个累计值：

```text
5
10
5
```

每次 Call 都发送 `{"n": 5}`。前两次复用 `orders-42` 的计数器；第三次使用 `orders-42-other`，状态独立。provider 持续运行时，再执行一次相同 consumer 命令，输出 `15`、`20`、`10`。provider 先输出 `registered`，首次调用才输出 `created` 和 `invoke`，退出时输出 `closed`。

## 实现

[provider.py](provider.py) 定义 `Counter.handle`，将整数 `n` 加到状态字段 `count` 并返回累计值，再注册无参工厂；SDK 管理对象创建、串行执行和响应。

[consumer.py](consumer.py) 使用 `Runtime.addr(...).call(...)` 连续调用同一个 Addr，再调用另一个 ID；累计值展示状态复用与隔离。`--count` 控制对第一个 ID 的调用次数，每次加 5。[gateway/main.go](gateway/main.go) 从环境读取 Key，装配共享 actorxdist.Runtime 与 HTTP handler，并按 Gateway → HTTP Server → Runtime 顺序关闭。

这是本机源码示例，当前使用 Call；provider 断线后需要重新启动注册。

Counter 只实现 `handle`，与实现 `handle_stream` 的 [Chat](../chat/README.zh.md) 使用相同的 `register(type, factory)`。一个 Actor 也可同时实现两者，通过同一个 Addr 共享状态。请求缺失操作会抛出带选定地址的 `UnsupportedOperationError`（1013），该 Addr 仍可执行已支持的操作。两种方法及错误处理见 [SDK 指南](../../sdk/python/README.zh.md)。

gateway 和 SDK 配套使用 `actorx.remote/v1`。若此 Type 之前已注册过仅 Call 绑定，为 provider 和 consumer 同时选择新 Type，例如 `demo.counter.v2`；详见[协议与 Type 迁移](../../docs/runtime.zh.md#协议与-type-迁移)。

## 链接

- [Python SDK：安装、API 与错误语义](../../sdk/python/README.zh.md)
- [Remote 协议](../../docs/protocol.zh.md)与[运行模型及生命周期](../../docs/runtime.zh.md)
