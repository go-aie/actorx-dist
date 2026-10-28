# Actorx Remote

[English](README.md) | 中文

Remote 让跨语言 Actor provider 和 consumer 通过 HTTP 接入 Go gateway。gateway 内部使用 dist/NATS，外部程序只需 gateway URL 和 API Key。

Python SDK 提供 `Runtime.register(type, factory)` 与 `Runtime.addr(type, id).call(message)`。注册安装创建能力，首次 Call 才按需创建业务对象；同一代实例在后续调用间保留状态。

从 [Ping/Pong 示例](examples/pingpong/README.zh.md)开始，查看安装、进程启动命令和预期输出。[Python SDK 指南](sdk/python/README.zh.md)说明具体 API 与异常。

| 文档 | 内容 |
| --- | --- |
| [协议](docs/protocol.zh.md) | JSON-RPC 方法、地址、HTTP/SSE、鉴权、错误与 wire 限制。 |
| [运行模型与生命周期](docs/runtime.zh.md) | 激活、固定代次、期限、并发、Go 接入和清理责任。 |

当前实现支持 HTTP/SSE Call，以及从源码安装的 Python SDK。Send、Stream、WebSocket、自动重试/重连和 provider 恢复不在当前实现范围内。
