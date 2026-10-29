# Actorx Distributed

[English](README.md) | 中文

Actorx Distributed 是一个基于 [Actorx](https://github.com/go-aie/actorx/blob/main/README.zh.md) 和 [NATS](https://nats.io/) 的 Go 分布式 Actor 框架，支持按类型和业务 ID 跨进程访问有状态 Actor，并在首次调用时按需创建实例。

Actorx 负责进程内的 Actor 执行，NATS 负责跨进程通信，JetStream KV 协调实例归属。Actor 支持 Send、Call 和 Stream，分别用于单向消息、请求响应和流式结果。

## 开始使用

以跨进程计数器为例：Actor 保存计数，收到 Add 消息后累加并返回结果。以下代码展示主要用法；包含生成绑定、错误处理和退出流程的完整可运行程序见 [Counter 示例](examples/counter/README.zh.md)。

```go
import (
    "github.com/go-aie/actorx"
    "github.com/go-aie/actorx-dist"
)

type Add struct {
    N int `json:"n"`
}

//actorx:actor
type Counter struct {
    count int
}

func (c *Counter) Add(_ actorx.Context, msg Add) (int, error) {
    c.count += msg.N
    return c.count, nil
}

var CounterSpec = actorxdist.ActorSpec[*Counter]{
    Type: "counter",
    New: func(actorxdist.ActorKey) *Counter {
        return &Counter{}
    },
}
```

`Counter` 定义业务行为，`CounterSpec` 声明类型和构造函数。工作进程注册后保持运行以处理请求，退出时由 `Runtime.Close` 统一撤销注册并等待 Actor 退出：

```go
ctx := context.Background()
worker, _ := actorxdist.NewRuntime(ctx)
defer worker.Close(ctx)
_, _ = worker.Register(ctx, CounterSpec)
```

另一个调用进程创建自己的 Runtime，无需注册 Actor，直接按类型和 ID 调用计数器：

```go
client, _ := actorxdist.NewRuntime(ctx)
defer client.Close(ctx)

addr, _ := client.Addr("counter", "orders-42")
n, _ := addr.Call[int](ctx, Add{N: 5})
fmt.Println(n) // 新计数器输出 5
```

`Addr("counter", "orders-42")` 只创建逻辑地址，不会启动 Actor。首次 Send、Call 或 Stream 才查找或激活实例，框架保证同一类型和 ID 的唯一归属。

## 实际运行

需要 Go 1.27 和 [NATS Server v2.10.18](https://github.com/nats-io/nats-server/releases/tag/v2.10.18)。安装适合自己平台的服务端，并确保 `nats-server` 在 PATH 中。以下命令均从 actorx-dist 仓库根目录运行。

### 启动 NATS

在终端 1 中使用开发配置启动 NATS，配置已开启 JetStream，监听本机 4222 端口：

```sh
nats-server -c examples/nats.conf -sd /tmp/dist-counter
```

保持该终端运行，供后续示例使用。

### 运行 Counter

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

调用方共执行两轮，每轮先 Send 增加 2，再 Call 增加 3 并返回累计值。保持同一工作进程运行，再次执行调用命令会输出 15、20：调用进程退出后，计数器的状态仍保存在工作进程中。

### 关闭程序

调用方完成后自动退出，不会停止计数器。结束示例时，在终端 2 按 Ctrl-C，等待工作进程及其 Actor 退出，再在终端 1 按 Ctrl-C 关闭 NATS。

状态保存在工作进程内存中，实例退出后不保留。失联时没有自动接管，强制退出后的归属恢复见[部署指南](docs/deployment.zh.md#显式恢复与存储修复)。

## 更多示例

各示例共用上述 NATS 服务，README 中包含各自的运行步骤：

| 示例 | 学习内容 |
| --- | --- |
| [Counter](examples/counter/README.zh.md) | 启动多个 worker，观察相同 ID 的唯一归属及不同 ID 的独立状态。 |
| [Client → Button → Counter](examples/click/README.zh.md) | 在独立进程间组合 Actor 调用。 |
| [Chat](examples/chat/README.zh.md) | 跨进程 Stream 和慢消费时的逐段输出。 |

## 深入了解

| 文档 | 内容 |
| --- | --- |
| [核心概念](docs/concepts.zh.md) | 逻辑对象、激活代次、地址与上下文。 |
| [调用语义](docs/runtime.zh.md) | Send/Call/Stream、期限、错误和容量。 |
| [生命周期](docs/lifecycle.zh.md) | Stop/Wait、对象寿命和关闭责任。 |
| [部署指南](docs/deployment.zh.md) | 配置、权限、契约升级与显式恢复。 |
| [跨语言接入](remote/README.zh.md) | HTTP/SSE Call/Stream、Python provider/consumer 与 remote 协议。 |

实现细节见[架构](docs/architecture.zh.md)、[wire 契约与资源限制](docs/contract.zh.md)、[Registry 协议](docs/registry.zh.md)，以及说明 namespace、subject 和权限的 [NATS 技术参考](docs/nats.zh.md)。
