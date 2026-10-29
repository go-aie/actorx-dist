# Counter 跨进程调用

[English](README.md) | 中文

Provider（提供方）保存计数状态，独立的 consumer（调用方）进程按类型和业务 ID 发送消息。重复运行调用方可观察状态保留，也可增加 provider 了解实例的唯一归属。

## 启动 NATS

需要 Go 1.27 和 [NATS Server v2.10.18](https://github.com/nats-io/nats-server/releases/tag/v2.10.18)。安装适合自己平台的服务端，并确保 `nats-server` 在 PATH 中。以下命令均从 actorx-dist 仓库根目录运行。

在终端 1 中使用[开发配置](../nats.conf)启动 NATS，配置已开启 JetStream，监听本机 4222 端口：

```sh
nats-server -c examples/nats.conf -sd /tmp/dist-counter
```

保持该终端运行，其他分布式示例也可共用此服务。如果运行过旧版单入口示例，请用 `-sd` 选择新的开发存储目录；共享 Go 类型移入可导入包后，示例契约已变化。

## 运行 Counter

### Provider（提供方）

在终端 2 中启动提供方进程，等待输出 `Counter provider ready`：

```sh
go run ./examples/counter/provider
```

### Consumer（调用方）

在终端 3 中运行调用方：

```sh
go run ./examples/counter/consumer -id orders-42
```

使用新的计数器 ID 时，输出：

```text
5
10
```

调用方共执行两轮，每轮先 Send 增加 2，再 Call 增加 3 并返回累计值。保持同一提供方进程运行，再次执行调用命令会输出 15、20：调用方进程退出后，计数器的状态仍保存在提供方进程中。Send 等待 mailbox 受理，Call 返回累计的业务结果。

## 多 provider 与不同 ID

保持原提供方进程运行，在另一个终端启动第二个 provider，使用相同类型及 NATS account：

```sh
go run ./examples/counter/provider
```

在调用方终端中使用不同 ID：

```sh
go run ./examples/counter/consumer -id another
```

对于新 ID，输出为 5、10，不同 ID 的状态彼此独立。KV 条件写为同一个 key 确定唯一拥有者，新 ID 可能归属任一 provider。再次调用 `orders-42` 时，仍由原拥有者继续累加已有计数。

## 关闭程序

调用方完成后自动退出，不会停止计数器。结束示例时，在各 provider 终端按 Ctrl-C，等待各自的 Runtime 及其 Actor 退出，再在终端 1 按 Ctrl-C 关闭 NATS。

状态保存在提供方进程内存中，实例退出后不保留。失联时没有自动接管，强制退出后的归属恢复见[部署指南](../../docs/deployment.zh.md#显式恢复与存储修复)。

## 实现说明

[actors.go](actors.go) 定义共享的 `counter` 包，包含 Add、Counter 和 CounterSpec。两个程序分别导入该包，各自管理 Runtime 的生命周期：

- [provider/main.go](provider/main.go) 创建 Runtime，注册 `counter.CounterSpec`，然后等待退出。
- [consumer/main.go](consumer/main.go) 创建 Runtime，取得地址并发送 Add 消息，不注册 Actor。

两端均支持 `-nats nats://SERVER:4222`。通过 `go generate ./examples/counter` 生成绑定；consumer 也会链接共享包中的 Actor 定义。远端 Stop/Wait 见[生命周期](../../docs/lifecycle.zh.md)，跨进程组合调用见 [Click](../click/README.zh.md)。

相同计数业务的 Python provider 与 HTTP consumer 接入方式见 [Remote Counter](../../remote/examples/counter/README.zh.md)。
