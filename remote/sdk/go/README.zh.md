# actorx remote Go SDK

[English](README.md) | 中文

通过 HTTP/SSE 编写远程 Actor provider（提供方）和 consumer（调用方），支持 Call 与 Stream。模块为 `github.com/go-aie/actorx-dist/remote/sdk/go`，包名为 `actorxremote`，需要 Go 1.27，仅依赖标准库。应用只需 gateway origin 与 API Key，无需安装 actorx、actorx-dist 根模块或 NATS 客户端。

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
| `addr.Stream(ctx, message, opts...)` | 惰性、单次执行的 `iter.Seq2[any,error]`；`WithStreamTimeout(d)` 接受 `(0, 60s]`，默认五秒 |
| `rt.WaitClosed(ctx)` | 等待关闭或 provider 断线，保留关闭原因 |
| `rt.Close(ctx)` | 拒绝新工作、取消并汇合任务、关闭连接并释放实例 |

普通 Go 结构体实现一种或两种接口，注册使用闭包，例如 `func() (any, error) { return &ChatActor{}, nil }`。无需嵌入 SDK 基类、声明 capabilities 或分别注册两种操作。Call 和 Stream 共用同一 Type/ID/generation 对象及实例锁；不同身份独立。注册成功时尚未构造对象。

每注册最多八个实例和八个执行槽，排队即占槽。ctx 取消或 RPC 超时不能强制停止用户 goroutine；handler 必须观察 ctx，实际退出才释放锁和槽。工厂、handler、序列化的 error/panic 使用固定诊断，不传播原文或堆栈。

`StreamWriter.Write` 继承 handler context，按完整编码后的 chunk 计费，可能等待 credit 或 POST。Write 出错后立即返回；即使作者忽略错误，SDK 也不会伪造成功。writer 仅在当前 handler 内按顺序使用，不支持并发 Write 或方法返回后写入。返回 nil 表示成功完成，不需要 Complete。

输入使用 `encoding/json.Marshal`，支持结构体、JSON 标签、指针、map、slice、json.Number、json.RawMessage 和标准自定义序列化。输出及 provider message 解码为 nil、bool、string、json.Number、[]any、map[string]any，保留数字精度，不恢复 Go 类型。SDK 对编码后 JSON 校验 UTF-8、重复键、深度和完整消息大小；不提供业务类型转换 API。

## 查询目录

```go
report, err := rt.Actors(ctx)
// Check err before using report.Actors.
```

查询分布式 Runtime 在采集窗口内观察到的兼容 Remote 类型，无需范围参数。同 Type 去重，来源描述不一致时为空。结果中的 `actors` 是类型声明，不是运行实例。响应只包含 `actors`。发现采用有限采集窗口，不保证观察到所有节点；底层快照或报告因限额截断时返回 1001/full，不返回部分目录。

查询总预算最多五秒，Runtime 关闭会取消并汇合查询；失败不返回部分目录，不自动重试。完整响应限 8 KiB，超出时 Gateway 拒绝响应。跨 Gateway 调用入口尚待补齐，当前目录可见不代表任意入口都可 Call/Stream 该类型。详见 [目录协议](../../docs/protocol.zh.md#runtimeactors列出已注册的-actor-类型)。

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

`Stream` 首次迭代才启动预算、取得输入快照并建立 HTTP 请求；开始迭代前须保持 message 及其引用数据稳定。建立和传输错误以 `(nil, err)` 交付，正常完成没有额外项。重复或并发执行同一序列返回本地 `ErrStreamUsed`，不重发请求、不影响首次执行。stream/opened 不固定 generation。

```go
for chunk, err := range actor.Stream(ctx, message) {
    if err != nil {
        return err
    }
    fmt.Println(chunk)
}
```

循环体正常返回（含 `continue`）才确认当前项；`break`、外层 `return` 和 panic 取消/丢弃当前项，不发成功 ACK，panic 清理后原样传播。每段独立使用四项/32 KiB 窗口，排队和活跃项均占额度。有效生产者失败先排空已接受数据，再报告原错误；取消、期限和损坏证据中断排空。EOF 不是成功；最后 ACK 失败只有在有效终态加本地完整交付后才保留原结果。总期限覆盖排队、建连、执行及消费，不逐项续期。

迭代过程负责清理，直到活跃循环体退出并汇合 reader/控制 POST，才释放 Addr 执行槽与 Runtime 工作登记。取消可以在循环体运行期间停止 IO；应用代码仍须配合 context。同一 Addr 的其他操作等待本次作用域结束。

退出时使用独立、尚未取消的清理 context 调用 Runtime.Close，并保留错误，例如 `cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)`。Close/WaitClosed 的等待 context 取消只结束本次等待，后台清理仍被 Runtime 持有，可以再次等待。忽略取消的 provider handler 或 consumer 循环体可能阻止完整关闭。

provider SSE 失败会关闭整个 Runtime，WaitClosed 返回原因；没有自动重连。纯 Call consumer 不建立 SSE，每个 consumer Stream 单独建立 SSE。当前没有 Send、WS、Call 的远端 cancel、重试或持久化恢复。

## 链接

- [Counter](../../examples/counter/README.zh.md) / [Chat](../../examples/chat/README.zh.md)：完整运行命令与源码。
- [运行模型](../../docs/runtime.zh.md) / [协议](../../docs/protocol.zh.md) / [Python SDK](../python/README.zh.md)。
- [维护与发布](../../docs/development.zh.md)：协议向量、分模块 CI、互通和发布消费边界。
