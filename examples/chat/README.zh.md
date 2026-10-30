# 跨进程流式聊天

[English](README.md) | 中文

从另一个进程中的 Assistant 逐词接收模拟回答，无需模型或凭据。业务流程与核心 [chat 示例](https://github.com/go-aie/actorx/blob/main/examples/chat/README.zh.md) 一致；actorx-dist 增加 NATS 传输、逻辑寻址和提供方进程注册。

需要 Go 1.27+。先启动[开发 NATS 服务](../counter/README.zh.md#启动-nats)，再从 actorx-dist 仓库根目录，在两个独立终端运行：

```sh
# 终端 1：等待 "Assistant provider ready"。
go run ./examples/chat/provider
# 终端 2：消费远端 Assistant 的流式回答。
go run ./examples/chat/consumer -id assistant-42
```

```text
answer: Actors use messages to coordinate work.
chunks: 6
```

[actors.go](actors.go) 定义共享的 `chat` 包，包含 Ask、TextChunk、Assistant 和 AssistantSpec。

- [provider/main.go](provider/main.go) 创建 Runtime，注册 `chat.AssistantSpec`，然后等待退出。
- [consumer/main.go](consumer/main.go) 创建 Runtime，通过 `Addr("chat.assistant", id)` 取得地址，用 Stream 逐段消费 TextChunk，不注册 Actor。

两条 consumer 命令均显式传入 `-id assistant-42`，它也是默认值；修改该值可选择另一个逻辑 Assistant。两个程序均导入共享 Actor 包，各自管理 Runtime 的生命周期。

`StreamReply.Write` 可能等待慢速调用方，因此 handler 使用 `actorx.StartTask`：任务使用自己的 context 写入分片，串行完成回调调用 `Complete(err)`。consumer 收到一段就输出一段，仅在 Stream 成功结束后输出分片数量。

通过慢消费观察逐段输出：

```sh
go run ./examples/chat/consumer -id assistant-42 -consume-delay 100ms
```

`-consume-delay` 让每个消费循环体保持指定时长。默认六个分片超过四条窗口，继续交付需要归还消费额度。这里演示慢消费；精确窗口限制及取消后的清理由专门的 Stream 测试验证，见[流语义](../../docs/runtime.zh.md#stream)。

consumer 请求的总期限为十秒，Ctrl-C 可取消请求；取消只请求远端工作停止，不证明远端清理已完成。consumer 关闭不停止远端 Assistant；provider 收到 Ctrl-C 后关闭 Runtime 并等待所属工作结束。所有角色都可通过 `-nats nats://SERVER:4222` 选择服务，须连接相同服务/account。

修改 handler 后，运行 `go generate ./examples/chat`。
