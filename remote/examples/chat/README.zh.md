# 远程 Actor 流式 Chat

[English](README.md) | 中文

Go 或 Python provider 分六段回复，consumer 逐段显示文本；同一 Actor 也提供 Call，演示共享会话状态。两端只需要 gateway URL 和 API Key，均主动连接 HTTP/SSE，不监听端口、不安装 NATS 客户端。

## 运行

需要 Go 1.27；使用 Python 一端时需要 Python 3.11+。gateway 内部需要支持 JetStream 的 `nats-server`。以下命令从仓库根目录执行。Go 示例是独立模块，仅依赖标准库和 SDK，源码开发通过本地 replace 引用 SDK。

```sh
python3 -m venv /tmp/actorx-remote-demo-venv
/tmp/actorx-remote-demo-venv/bin/python -m pip install -e remote/sdk/python
```

用 `secrets.token_urlsafe(32)` 生成随机 API Key，通过受信环境给 gateway、provider、consumer 终端注入相同的 `REMOTE_API_KEY`。Key 不作为命令行参数。

终端 1，启动独立开发存储上的 NATS：

```sh
nats-server -c examples/nats.conf -sd /tmp/actorx-remote-chat
```

终端 2，启动共用 gateway：

```sh
GOWORK=off go run ./remote/examples/counter/gateway \
  -nats nats://127.0.0.1:4222 -listen 127.0.0.1:8080
```

终端 3，选择一种 provider：

```sh
# Go
(cd remote/examples/chat/go && GOWORK=off go run ./provider)

# Python
/tmp/actorx-remote-demo-venv/bin/python remote/examples/chat/python/provider.py
```

终端 4，选择一种 consumer：

```sh
# Go
(cd remote/examples/chat/go && GOWORK=off go run ./consumer)

# Python
/tmp/actorx-remote-demo-venv/bin/python remote/examples/chat/python/consumer.py
```

provider 和 consumer 可独立选择 Go 或 Python，无需修改 gateway。

两端都支持 `--server` 和 `--type`，默认分别为 `http://127.0.0.1:8080`、`demo.chat`。更改 Type 时两端保持一致；consumer 的 `--id` 选择逻辑实例。每个 Type 同时只运行一个 provider。consumer 完成后退出，按 provider → gateway → NATS 的顺序 Ctrl-C 关闭。

## 预期输出

首次使用一个 ID 时：

```text
call turn: 1
answer: Actors use messages to coordinate work.
```

consumer 先 Call，再通过同一 Actor 流式获取回答。provider 输出 `Chat turn 1` 和 `Chat turn 2`；再次使用同一 ID 时，轮次继续递增。

## 实现

[Go provider](go/provider/main.go) 同时实现 `Actor.Handle` 与 `StreamActor.HandleStream`，后者调用 `StreamWriter.Write`；[Python provider](python/provider.py) 实现 `handle` 与异步生成器 `handle_stream`。两种方法共享 `turns` 字段和实例锁。无需模型服务。

[Go consumer](go/consumer/main.go) 使用 `for chunk, err := range actor.Stream(ctx, message)`，退出循环时自动清理；[Python consumer](python/consumer.py) 使用 async context 和 async for。两种语言都在结束时关闭流。

这是本机源码示例。Go 示例的本地 replace 不是发行包证据；provider 不自动重连。gateway 与两种 SDK 配套使用 `actorx.remote/v1`。如已有旧 Call-only Type 契约，为两端同时选择新 Type，不清理共享 Registry；详见[协议与 Type 迁移](../../docs/runtime.zh.md#协议与-type-迁移)。

## 链接

- [Go SDK](../../sdk/go/README.zh.md) / [Python SDK](../../sdk/python/README.zh.md)
- [Remote](../../README.zh.md) / [运行模型](../../docs/runtime.zh.md)
