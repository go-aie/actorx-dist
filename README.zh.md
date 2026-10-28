# Actorx Distributed

[English](README.md) | 中文

Actorx Distributed（Go 包名 `actorxdist`）是一个基于 [actorx](https://github.com/go-aie/actorx/blob/main/README.zh.md) 的 Go 分布式 Actor 框架，支持逻辑寻址、按需激活、唯一归属、跨进程 Send/Call/Stream，以及停止和完整退出管理。

业务沿用 actorx 的有状态对象、串行消息处理、异步任务和流式结果。dist 通过 NATS 与 JetStream KV 管理通信和归属，让调用方按类型和业务 ID 访问对象。

## 开始使用

以跨进程计数器为例：Actor 保存计数，收到 Add 消息后累加并返回结果。

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

Counter 定义业务行为，CounterSpec 声明逻辑类型和构造函数。工作进程注册 Spec，并保持运行以处理后续请求；退出时 Runtime.Close 统一撤销注册并等待实例收尾。下面省略常规错误处理及等待退出的代码：

```go
ctx := context.Background()
worker, _ := actorxdist.NewRuntime(ctx)
defer worker.Close(ctx)
_, _ = worker.Register(ctx, CounterSpec)
```

另一个调用进程创建自己的 Runtime，无需注册 Actor。以下假设已取得请求上下文 ctx，且目标 ID 尚未使用：

```go
client, _ := actorxdist.NewRuntime(ctx)
defer client.Close(ctx)

addr, _ := client.Addr("counter", "orders-42")
n, _ := addr.Call[int](ctx, Add{N: 5})
fmt.Println(n) // 5
```

`Addr("counter", "orders-42")` 只取得逻辑地址；首次调用才查找或激活对象，框架保证同一逻辑身份的唯一归属。地址选定一次激活后始终固定该代；实例退出后，使用新地址选择下一代。

Actor 内寻址使用 `actorxdist.MustFrom(ctx).Addr(type, id)`，Self 可取得当前代回信地址，详见[核心概念](docs/concepts.zh.md)。上述代码需要生成 Bindings；完整定义、绑定和进程入口见 [Counter 示例](examples/counter/README.zh.md)。

## 实际运行

需要 Go 1.27。从 actorx-dist 仓库根目录运行；go.mod 已固定发布的 actorx 核心版本。

### 启动 NATS

先从 [NATS 官方 v2.10.18 发布页](https://github.com/nats-io/nats-server/releases/tag/v2.10.18)下载适合系统和架构的压缩包。以下以 macOS Apple Silicon 为例，下载、解压并将可执行文件加入当前终端 PATH；其他平台在同一发布页选择对应包：

```sh
curl -fL -o /tmp/nats-server.tar.gz \
  https://github.com/nats-io/nats-server/releases/download/v2.10.18/nats-server-v2.10.18-darwin-arm64.tar.gz
tar -xzf /tmp/nats-server.tar.gz -C /tmp
export PATH="/tmp/nats-server-v2.10.18-darwin-arm64:$PATH"
```

在终端 1 中进入 actorx-dist 仓库根目录，启动开发服务。配置已开启 JetStream，监听本机 4222 端口；存储目录须可写：

```sh
nats-server -c examples/nats.conf -sd /tmp/dist-counter
```

保持终端 1 中的服务运行，后续示例共用这个 NATS 服务。

### 运行 Counter

打开终端 2，进入 actorx-dist 仓库根目录并启动工作进程，等待输出 `Counter worker ready`：

```sh
go run ./examples/counter -role worker
```

打开终端 3，同样进入 actorx-dist 仓库根目录并运行调用方：

```sh
go run ./examples/counter -role client -id orders-42
```

首次使用该 ID 时，输出：

```text
5
10
```

client 每轮先 Send 增加 2，再 Call 增加 3 并返回累计值，共执行两轮。同一 worker 仍存活时，在终端 3 再次运行相同命令，输出 15、20：调用进程虽然退出，远端 Counter 的状态仍保存在 worker 中。

### 关闭程序

client 完成调用后自动退出，不会停止远端 Counter。结束示例时，在终端 2 按 Ctrl-C，等待 worker 关闭并完成所属实例的退出；最后在终端 1 按 Ctrl-C 关闭 NATS。

状态保存在 worker 内存中，实例退出后不保留。失联时没有自动接管，强制退出后的归属恢复见[部署指南](docs/deployment.zh.md#显式恢复与存储修复)。

### 更多示例

继续尝试前，保持或重新启动上述 NATS 服务。各示例的 README 给出所需进程及命令：

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

实现细节见[架构与核心接入](docs/architecture.zh.md)、[wire 契约与资源限制](docs/contract.zh.md)、[Registry 协议](docs/registry.zh.md)。真实发布版本消费、实际跨主机网络与复制配置尚未验证。
