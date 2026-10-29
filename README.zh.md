# Actorx Distributed

[English](README.md) | 中文

Actorx Distributed 是一个基于 [Actorx](https://github.com/go-aie/actorx) 和 [NATS](https://nats.io/) 的 Go 分布式 Actor 框架，支持按类型和业务 ID 跨进程访问有状态 Actor，并在首次调用时按需创建实例。

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

状态保存在工作进程内存中，实例退出后不保留。失联时没有自动接管，强制退出后的归属恢复见[部署指南](docs/deployment.zh.md#显式恢复与存储修复)。

## 示例

建议从 Counter 开始，其中包含环境准备、运行命令、预期输出和关闭流程。各示例可共用同一个 NATS 服务。

| 示例 | 学习内容 |
| --- | --- |
| [Counter](examples/counter/README.zh.md)（建议从这里开始） | 跨进程调用、状态保留与多 worker。 |
| [Click](examples/click/README.zh.md) | Button 调用 Counter，组合跨进程 Actor 调用。 |
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
