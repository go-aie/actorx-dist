# Python 远程 Actor 流式 Chat

[English](README.md) | 中文

Python provider 用异步生成器分六段回复，consumer 逐段显示文本。Go gateway 通过内部 actorx-dist/NATS 路由；两个 Python 进程只需要 gateway URL 和同一个 API Key，无需接入模型或安装 NATS 客户端。

## 运行

需要 Go 1.27、Python 3.11+ 和支持 JetStream 的 `nats-server`。以下命令从仓库根目录执行：

```sh
python3 -m venv /tmp/actorx-remote-demo-venv
/tmp/actorx-remote-demo-venv/bin/python -m pip install -e remote/sdk/python
```

用 `secrets.token_urlsafe(32)` 生成随机 API Key，通过受信环境为 gateway、provider、consumer 三个终端注入相同的 `REMOTE_API_KEY`。Key 不作为命令行参数。

终端 1，启动内部 NATS：

```sh
nats-server -c examples/nats.conf -sd /tmp/actorx-remote-chat
```

终端 2，启动与 Ping/Pong 共用的通用 gateway：

```sh
GOWORK=off go run ./remote/examples/pingpong/gateway \
  -nats nats://127.0.0.1:4222 -listen 127.0.0.1:8080
```

终端 3，注册流式 provider：

```sh
/tmp/actorx-remote-demo-venv/bin/python remote/examples/chat/provider.py
```

终端 4，消费回复：

```sh
/tmp/actorx-remote-demo-venv/bin/python remote/examples/chat/consumer.py
```

增加 `--consume-delay 0.1`，让每项处理耗时 100ms；处理完当前项、请求下一项时 SDK 才发送消费确认。consumer 完成后退出；按 provider → gateway → NATS 的顺序 Ctrl-C 关闭其余进程。

## 预期输出

文本逐段出现，完成后显示总项数：

```text
answer: Actors use messages to coordinate work.
chunks: 6
```

provider 首次输出 `Chat actor created` 和 `Chat turn 1`。再次运行相同 ID 的 consumer 会输出 `Chat turn 2`，复用同代实例；`--id another` 创建另一个实例。

## 实现

[provider.py](provider.py) 实现 `handle_stream(message)` 异步生成器，与 Ping/Pong 一样使用 `register(type, factory)`。注册保存工厂，首次 Stream 才创建对象。六段固定文本超过四项发送窗口，慢消费会经 gateway 和 actorx-dist 传递背压。

[consumer.py](consumer.py) 使用 `async with actor.stream(...) as chunks` 和 `async for`。正常迭代耗尽表示收到明确终态且全部项已处理；中途 `break`、异常或 Runtime 关闭会取消未完成流。每个流默认总期限五秒，`timeout` 可设为至多 60 秒；本示例为 30 秒。

这是本机源码示例；不自动重连、重试或续读。consumer 结束不停止 provider。

ChatActor 只实现 `handle_stream`，也可增加 `handle` 返回单个 JSON 结果；两种操作共享同一实例并串行执行。对当前仅 Stream 的示例调用 `.call(...)` 会抛出带选定地址的 `UnsupportedOperationError`（1013），之后同一 Addr 仍可使用 Stream。两种方法共存的示例见 [SDK 指南](../../sdk/python/README.zh.md)。

gateway 和 SDK 配套使用 `actorx.remote/v1`。已有 Stream-capable Type 在 DTO 和绑定不变时保持 actorx-dist 契约；原 Call-only Type 须换用新 Type。详见[协议与 Type 迁移](../../docs/runtime.zh.md#协议与-type-迁移)。

## 链接

- [Python SDK](../../sdk/python/README.zh.md)
- [Remote 协议：Stream](../../docs/protocol.zh.md#stream-过程)与[运行模型](../../docs/runtime.zh.md)
- [Call 版 Ping/Pong](../pingpong/README.zh.md)
