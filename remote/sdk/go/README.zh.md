# actorx remote Go SDK

[English](README.md) | 中文

通过 HTTP/SSE 编写远程 Actor provider 和 consumer，支持 Call 与 Stream。模块为 `github.com/go-aie/actorx-dist/remote/sdk/go`，包名为 `actorxremote`，需要 Go 1.27，仅依赖标准库。应用只需 gateway origin 与 API Key，无需安装 actorx、actorx-dist 根模块或 NATS 客户端。

## 运行

当前从源码使用，尚未验证公开发行包。完整的独立进程命令见 [Counter](../../examples/counter/README.zh.md) 与 [Chat](../../examples/chat/README.zh.md)，支持两端独立选择 Go/Python。已有 gateway 和 Go provider 时，从仓库根目录运行 consumer：

```sh
(cd remote/examples/counter/go && GOWORK=off go run ./consumer)
```

通过环境向各进程注入同一个 `REMOTE_API_KEY`。`--server` 默认 `http://127.0.0.1:8080`，`--type` 默认 `demo.counter`。

在自己的 Go 项目中引用当前源码：

```sh
go mod init example.com/my-remote-app
go mod edit -require=github.com/go-aie/actorx-dist/remote/sdk/go@v0.0.0
go mod edit -replace=github.com/go-aie/actorx-dist/remote/sdk/go=/absolute/path/to/actorx-dist/remote/sdk/go
```

导入入口：

```go
import actorxremote "github.com/go-aie/actorx-dist/remote/sdk/go"
```

SDK 可独立测试，不需要 gateway、NATS 或监听端口：

```sh
cd remote/sdk/go
GOWORK=off go test -race ./...
```

## 预期结果

新 Counter 的 consumer 输出 `5`、`10`、`5`；重复运行同一 ID 输出 `15`、`20`、`10`。Chat 输出六个 chunk，文本为 `Actors use messages to coordinate work.`；同一 Addr 的 Call 和 Stream 使用连续 turn。两种语言的输出一致。

## API 与实现

| API | 用法与语义 |
| --- | --- |
| `NewRuntime(origin, Options{APIKey: key})` | origin 为 HTTP/HTTPS origin，只做本地校验；Runtime 拥有 HTTP transport |
| `rt.Register(ctx, actorType, factory, opts...)` | 注册 provider；ctx 只限制安装，最多五秒；可选 `WithDescription` |
| `ActorFactory func() (any, error)` | 首次 create 才调用，必须短时返回，实现 Actor 或 StreamActor 的非 nil 对象 |
| `Actor.Handle(ctx, message) (any, error)` | Call handler，返回一个 JSON 值 |
| `StreamActor.HandleStream(ctx, message, writer) error` | Stream handler，逐项 `writer.Write(value)`，返回后由 SDK 提交终态 |
| `rt.Addr(type, id)` / `addr.Address()` | 本地寻址 / 独立地址快照，generation 初始为空 |
| `addr.Call(ctx, message)` | 五秒总预算，包含 Addr 排队及 HTTP，尊重更早 context 期限 |
| `addr.Stream(ctx, message, opts...)` | 建立并验证 stream/opened；`WithStreamTimeout(d)` 接受 `(0, 60s]`，默认五秒 |
| `stream.Next()` / `Value()` / `Err()` | 单消费者迭代，Next 为 false 后检查 Err；Value 为通用 JSON 值 |
| `stream.Close()` | 幂等关闭，可并发中断 Next；提前 break 时必须调用 |
| `rt.WaitClosed(ctx)` | 等待关闭或 provider 断线，保留关闭原因 |
| `rt.Close(ctx)` | 拒绝新工作、取消并汇合任务、关闭连接并释放实例 |

普通 Go 结构体实现一种或两种接口，注册使用闭包，例如 `func() (any, error) { return &ChatActor{}, nil }`。无需嵌入 SDK 基类、声明 capabilities 或分别注册两种操作。Call 和 Stream 共用同一 Type/ID/generation 对象及实例锁；不同身份独立。注册成功时尚未构造对象。

每注册最多八个实例和八个执行槽，排队即占槽。ctx 取消或 RPC 超时不能强制停止用户 goroutine；handler 必须观察 ctx，实际退出才释放锁和槽。工厂、handler、序列化的 error/panic 使用固定诊断，不传播原文或堆栈。

`StreamWriter.Write` 继承 handler context，按完整编码后的 chunk 计费，可能等待 credit 或 POST。Write 出错后立即返回；即使作者忽略错误，SDK 也不会伪造成功。writer 仅在当前 handler 内按顺序使用，不支持并发 Write 或方法返回后写入。返回 nil 表示成功完成，不需要 Complete。

输入使用 `encoding/json.Marshal`，支持结构体、JSON 标签、指针、map、slice、json.Number、json.RawMessage 和标准自定义序列化。输出及 provider message 解码为 nil、bool、string、json.Number、[]any、map[string]any，保留数字精度，不恢复 Go 类型。SDK 对编码后 JSON 校验 UTF-8、重复键、深度和完整消息大小；不提供业务类型转换 API。

## 地址与错误

用 `errors.Is` 分类，`errors.As` 读取 `*RemoteError` 的 Code、RPCCode、Data，或 `*TransportError` 的 Code、Status。用户取消保留 `context.Canceled`；其他分类包括 `ErrDeadlineExceeded`、`ErrOutcomeUnknown`、`ErrActivationUnknown`、`ErrAddressUncertain`、`ErrUnsupportedOperation`、`ErrStopped`、`ErrAuthentication`、`ErrSession`、`ErrProtocol` 和 `ErrTransport`。

首次可信成功或错误保存 generation，后续操作固定该代次。缺失方法返回 1013/unsupported_operation，必须带完整地址，Stream 必须零项且 final_seq=0。SDK 先保存 generation，再返回错误；Counter 的 Stream 失败后，原 Addr 仍可 Call。示例：

```go
if errors.Is(err, actorxremote.ErrUnsupportedOperation) {
    var detail *actorxremote.RemoteError
    if errors.As(err, &detail) {
        fmt.Println(detail.RPCCode, actor.Address().Generation)
    }
}
```

可能已受理却无可信选代证据时，Addr 进入 uncertain，后续操作本地返回 `ErrAddressUncertain`。重新创建 Addr 不会对之前的工作去重；固定地址不自动换代。HTTP 200/204 不能替代业务终态，不自动重试。

## 流与关闭

`Stream` 返回前校验 stream/opened，建立前的 JSON RPC error 直接返回；stream/opened 不固定 generation。Next 交付一项，再次 Next 才确认前项消费，处理最后一项后也需再次 Next。每段窗口独立为四项/32 KiB，排队和当前项均占额度。正常耗尽要求明确终态，EOF 不是成功；部分失败不回滚已消费内容。总期限覆盖排队、建连、执行及消费，不逐项续期。

Next/Value/Err 由单消费者使用；Close 可与 Next 并发，负责取消未完成流并汇合 reader。总是 defer stream.Close，提前 break 也能清理。流在结束或关闭前持有 Addr 的执行所有权；同一 Addr 的其他操作需等待。

退出时使用独立、尚未取消的清理 context 调用 Runtime.Close，并保留错误，例如 `cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)`。Close/WaitClosed 的等待 context 取消只结束本次等待，后台清理仍被 Runtime 持有，可以再次等待。忽略取消的用户代码可能阻止完整关闭。

provider SSE 失败会关闭整个 Runtime，WaitClosed 返回原因；没有自动重连。纯 Call consumer 不建立 SSE，每个 consumer Stream 单独建立 SSE。当前没有 Send、WS、Call 的远端 cancel、重试或持久化恢复。

## 链接

- [Counter](../../examples/counter/README.zh.md) / [Chat](../../examples/chat/README.zh.md)：完整运行命令与源码。
- [运行模型](../../docs/runtime.zh.md) / [协议](../../docs/protocol.zh.md) / [Python SDK](../python/README.zh.md)。
- [维护与发布](../../docs/development.zh.md)：协议向量、分模块 CI、互通和发布消费边界。
