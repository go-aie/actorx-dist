# Counter 跨进程调用

[English](README.md) | 中文

工作进程保存计数状态，独立的调用进程按类型和业务 ID 发送消息。重复运行调用方可观察状态保留，也可增加 worker 了解实例的唯一归属。

## 启动 NATS

需要 Go 1.27 和 [NATS Server v2.10.18](https://github.com/nats-io/nats-server/releases/tag/v2.10.18)。安装适合自己平台的服务端，并确保 `nats-server` 在 PATH 中。以下命令均从 actorx-dist 仓库根目录运行。

在终端 1 中使用[开发配置](../nats.conf)启动 NATS，配置已开启 JetStream，监听本机 4222 端口：

```sh
nats-server -c examples/nats.conf -sd /tmp/dist-counter
```

保持该终端运行，其他分布式示例也可共用此服务。

## 运行 Counter

在终端 2 中启动工作进程，等待输出 `Counter worker ready`：

```sh
go run ./examples/counter -role worker
```

在终端 3 中运行调用方：

```sh
go run ./examples/counter -role client -id orders-42
```

使用新的计数器 ID 时，输出：

```text
5
10
```

调用方共执行两轮，每轮先 Send 增加 2，再 Call 增加 3 并返回累计值。保持同一工作进程运行，再次执行调用命令会输出 15、20：调用进程退出后，计数器的状态仍保存在工作进程中。Send 等待 mailbox 受理，Call 返回累计的业务结果。

## 多 worker 与不同 ID

保持原工作进程运行，在另一个终端启动第二个 worker，使用相同类型及 NATS account：

```sh
go run ./examples/counter -role worker
```

在调用方终端中使用不同 ID：

```sh
go run ./examples/counter -role client -id another
```

对于新 ID，输出为 5、10，不同 ID 的状态彼此独立。KV 条件写为同一个 key 确定唯一拥有者，新 ID 可能归属任一 worker。再次调用 `orders-42` 时，仍由原拥有者继续累加已有计数。

## 关闭程序

调用方完成后自动退出，不会停止计数器。结束示例时，在各 worker 终端按 Ctrl-C，等待各自的 Runtime 及其 Actor 退出，再在终端 1 按 Ctrl-C 关闭 NATS。

状态保存在工作进程内存中，实例退出后不保留。失联时没有自动接管，强制退出后的归属恢复见[部署指南](../../docs/deployment.zh.md#显式恢复与存储修复)。

## 实现说明

[actors.go](actors.go) 定义 Add、Counter 和 CounterSpec；[main.go](main.go) 负责参数、Runtime 生命周期及两个进程角色：worker 通过 `rt.Register(ctx, CounterSpec)` 注册创建能力并等待退出；client 不注册 Actor，只调用远端 Counter。默认角色为 client。所有进程可通过 `-nats nats://SERVER:4222` 连接同一服务。

CounterSpec 与 Counter 一起定义，通过 `go generate ./examples/counter` 生成绑定。client 角色仍链接本程序中的 Actor 定义。远端 Stop/Wait 见[生命周期](../../docs/lifecycle.zh.md)，跨进程组合调用见 [Click](../click/README.zh.md)。

相同计数业务的 Python provider 与 HTTP consumer 接入方式见 [Remote Counter](../../remote/examples/counter/README.zh.md)。
