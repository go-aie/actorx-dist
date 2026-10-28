# Remote 运行模型与生命周期

[English](runtime.md) | 中文

Remote 在 actorx-dist 之上提供跨语言接入。[协议文档](protocol.zh.md)定义 wire 契约，[Counter 示例](../examples/counter/README.zh.md)提供可运行命令。

## 组件与激活

```text
consumer SDK → HTTP Call/Stream → Gateway → actorx-dist ProxyActor
                                              ↓
provider 对象 ← provider SDK ← 注册 SSE create/invoke/invoke_stream
                       ↓
                HTTP 结果/分片 POST → Gateway → consumer 结果/分片
```

| 组件 | 职责 |
| --- | --- |
| actorx | mailbox、Deferred Reply、自有任务与本地生命周期 |
| actorx-dist | Type/ID 寻址、选代、代理唯一归属、Call 与 Stream |
| remote | Gateway、注册、provider 会话、ProxyActor 与响应关联 |
| remote/http | 鉴权、路由、JSON 转换、SSE 和连接/写期限 |
| Go/Python SDK | Actor 实例创建、串行 handler、地址状态与 HTTP/SSE 传输 |

gateway 借用已有的一个 `*actorxdist.Runtime`，外部程序无需 NATS 客户端或监听端口。HTTP/SSE 是接入适配，不是新增的 actorx Channel 实现。SDK Runtime 管理外部连接及 Python 对象，actorxdist.Runtime 管理代理归属；SDK 不 claim 归属，也不生成 actorx-dist generation。

Provider 通过 SDK 注册 Actor 并建立注册 SSE，在校验注册结果后返回，此时尚未创建业务对象。`addr(type, id)` 只创建本地句柄。首次 Call 或 Stream 由 actorx-dist 查找或激活 ProxyActor；代理在 `Started` 中取得完整 Self 地址并等待 `actor/create` 成功，之后才能执行 `actor/invoke` 或 `actor/invoke_stream`。已有就绪代理则省略创建。

SDK 按本次注册和完整地址（Type/ID/generation）保存对象。同一身份的重复 create 复用对象，并响应新的 RPC ID；不同 ID 或代次使用独立对象。注册时不创建对象；Python SDK 在需要实例时才调用注册时传入的类或创建函数，`factory` 参数约束见 [SDK 指南](../sdk/python/README.zh.md#api-与类型)。首次 create 要求真实对象至少有一个可调用的 `handle` / `handle_stream`，否则返回创建失败 1000。

Python 统一通过 `runtime.register` 注册 Actor，没有 `stream` 开关。Call/Stream 复用真实对象、实例锁和执行槽，在锁内检查所请求的方法。`handle(message)` 的结果必须可 await 并产出一个 JSON 值；`handle_stream(message)` 必须直接返回异步迭代器，可以是异步生成器或自定义迭代器。方法已调用但返回形态非法或业务抛异常时返回 1000，不改换通信操作。`Actor` / `StreamActor` 仅用于静态类型提示，不划分注册或生命周期。同实例 handler 串行执行，不同实例独立推进。

## 地址与错误

SDK 在返回值或抛 RPC 异常前，先保存 `result.address` 或 `error.data.address` 中有效的已选代地址，因此业务失败也可能固定 Addr 的 generation。固定后拒绝不同代次，不会隐式向新代重试。

实例缺失对应可调用方法时，返回带完整地址的 1013 `UnsupportedOperationError`；Stream 必须是 `final_seq: 0` 的零项终态。SDK 保存该 generation，同一个 Addr 随后可以调用已实现的另一种操作。1013 证明所请求的方法未进入，但构造对象仍可能发生；缺少地址或矛盾的 Stream 终态不作为可信 1013，按 1004 处理。

首次 Call 或 Stream 可能已受理却缺少有效选代证据时，句柄进入不明状态。本次报告不明分类（或保留调用者取消），后续调用在本地抛 `AddressUncertainError`。重新取得句柄是调用者的显式决定，不会对之前的工作去重。鉴权/参数拒绝和确定的建连失败不污染句柄。

Python `RemoteError.code` 为字符串 kind，`rpc_code` 为 wire 整数。已知 kind/code 不一致属于协议错误；未知合法 wire code 使用通用 RemoteError 和 `code="unknown"`。HTTP 鉴权/会话错误使用 `AuthenticationError`/`SessionError`，传输和损坏响应分别使用对应异常。完整 API 见 [SDK 指南](../sdk/python/README.zh.md)。

## 期限与并发

SDK Call 的五秒总预算覆盖句柄锁、HTTP 发送与响应。本地使用单调时钟，wire 携带绝对 UTC `deadline_unix_ms`。gateway 取 HTTP 入口五秒预算、请求指定期限、父 context 期限的最早者；已过期请求在投递前失败。后续阶段沿用剩余期限，不重新开启 Call 预算。

创建属于 actorx-dist 共享激活，代理 `Started` 的 create 等待上限为三秒或更早的生命周期期限。单个 consumer 取消只限制自己等待就绪的时间，不直接取消共享激活。注册成功后，五秒注册等待结束，SSE 转为 Runtime 持有的长连接。本机示例尚未验证跨主机时钟假设。

provider 执行规则：

1. 等待实例锁、handler 执行、响应 POST 共用传入期限，请求排队即占执行槽。执行前容量拒绝为 1001。
2. handler 启动前过期为 1005，该请求之后也不能执行；明确的启动前取消为 1009。
3. 启动后超时/取消且缺少有效终态为 1004。SDK 请求任务取消并结清 RPC，但直到 handler 实际退出才释放实例锁和执行槽。
4. 最终响应最多提交一次，且限于原预算内；不为报告过期开新预算或重试。结清后 handler 才返回时，只释放资源，不发送第二份结果。

SSE 读取和响应 I/O 不受实例锁阻塞。不配合取消的 handler 会继续占用实例，也可能阻止 SDK close 完成。客户端返回、await 被取消、provider EOF 均不证明外部副作用已停止。Call 没有远端 cancel 方法；Stream 取消仅针对本次请求。

Go 代理每实例只受理一个活动业务操作（Call/Stream），重叠调用返回 full；SDK 串行锁还保护 RPC 已结清但任务仍未退出的情况。同一个 Python Addr 会串行自己的调用，但不同句柄之间不共享客户端锁。

## 资源限制

| 资源 | 当前上限 |
| --- | --- |
| Gateway 活动业务操作（Call/Stream） | 整个 Gateway 共 8 个 |
| 活动 consumer 流 | 整个 Gateway 共 8 个 |
| 每注册出站消息 | 统一有界队列 64 项，包含 credit/cancel 预留 |
| 每注册 pending create / invoke | 各 8 项 |
| 每注册保留的激活记录 | 8 条 |
| 每实例代理活动业务操作（Call/Stream） | 1 个 |
| 每注册 Python 对象 / 执行槽 | 各 8 个 |

执行槽包括排队请求和超时后仍未退出的 handler，仅结清 RPC 不能绕过容量。actorx-dist 有独立的[执行和传输限制](../../docs/runtime.zh.md)，默认实例容量 64、payload 上限 64 KiB；这些与 remote 完整消息的 [wire 限制](protocol.zh.md)分别校验。当前 remote 常量不提供公共调节选项。

已校验的创建失败按注册及完整地址保留到清理和相关外层 Call 结束，避免稍后的停止/取消抹掉确定失败。执行派发/终态证据按 gateway 请求 ID 保留到对应 Call 结清。这些记录用于错误分类，成功值仍经普通 actorx-dist Result DTO 返回。

## Go 接入与所有权

Go 应用可通过独立的 [Go SDK](../sdk/go/README.zh.md) 经 HTTP/SSE 注册和调用 Actor，仅依赖标准库，与 Python SDK 使用相同 gateway。SDK 运行时拥有连接和实例，使用 context 控制操作并显式 Close。

嵌入 gateway 的 Go 服务端代码属于 actorx-dist 根模块。[Gateway](../gateway.go)接入已有 Runtime；[remotehttp.NewHandler](../http/handler.go)返回 `net/http.Handler`，不自行启动服务。HTTP 包导入名为 `remotehttp`。

| API | 所有权与完成语义 |
| --- | --- |
| `remote.NewGateway(rt)` | 借用 Runtime，由创建者负责最终关闭 |
| `Gateway.Register(ctx, params)` | ctx 限制安装，不持有后续 provider 连接寿命 |
| `Registration.Info()` | 不可变注册数据，含 session token |
| `Registration.Events()` | 一个传输读取者；session 结束受理时关闭 |
| `Registration.Done()` | actorx-dist Unregister 及所有已受理外层 Call 清理结束后关闭 |
| `Registration.Close(ctx)` | 幂等关闭 session 并等待所属清理 |
| `Gateway.Accept(ctx, token, response)` | 校验/关联 provider 响应，不执行用户 handler |
| `Gateway.Call(ctx, params)` | 通过 actorx-dist Call，错误时也保留选代地址 |
| `Gateway.Actors()` | 当前 Gateway 中已注册 Actor 的独立快照，不枚举运行中的实例 |
| `Gateway.Close(ctx)` | 拒绝新工作、等待所属注册，不关闭借用 Runtime |
| `remotehttp.NewHandler(gateway, options)` | 要求 `Options{APIKey: key}`，不接管 Gateway/Runtime 所有权 |

Register 在开始安装后，可能同时返回非空句柄和错误；调用方仍须 Close 并等待该句柄。关闭超时只结束本次等待，清理责任仍保留，可再次等待。

SSE 要求 ResponseController 写期限与 Flush；包装 ResponseWriter 的中间件应实现 `Unwrap`。使用逐帧期限，短的全局 WriteTimeout 会中断健康注册。[gateway 入口](../examples/counter/gateway/main.go)展示装配与独立清理 context。

## 断线与关闭

provider HTTP 请求取消、写失败或 SSE EOF 会发起注册关闭：撤目录、拒绝新工作、唤醒 pending、Unregister 所属代理。token 随之不可用；清理期间本地同 Type 注册仍冲突。SDK 在 provider 流失败后关闭，不自动重新注册。

按以下顺序关闭，各阶段使用独立、有界且尚未取消的 context：

1. `Gateway.Close`：结束注册和 provider SSE，等待已受理工作。
2. `http.Server.Shutdown`：等待 HTTP handler。
3. 创建者调用 `actorxdist.Runtime.Close`：关闭借用的分布式 Runtime。

Python `Runtime.close()` 关闭连接、取消并汇合 SDK 自有任务，然后释放对象引用。取消某个 close 等待者不丢失清理责任。当前不提供 Python 状态持久化、provider 接管、自动重放或租约恢复。静态 API Key 不热轮换或自动到期，撤销还需终止现有注册连接。

独立进程示例证明本机源码链路，不代表已验证公开发行包、跨平台或生产恢复保证。

## 协议与 Type 迁移

Actor 目录统一使用 `runtime/actors` 和结果字段 `actors`，Go 接口为 `Gateway.Actors()`；不可用错误为 `actor_unavailable`（1002）。早期开发名称已移除，目录与错误处理的调用方需同步更新；本次命名调整不改变 actorx-dist DTO 或 Type 契约。

当前仍处于早期开发，统一后的 gateway 和 Go/Python SDK 保留 `actorx.remote/v1`，两端实现须配套使用。SDK 每次 POST 自动携带 `X-Actorx-Remote-Protocol: actorx.remote/v1`，并校验注册结果；HTTP 在鉴权后、分派前对缺失或不匹配的版本返回 400。删除原注册参数 `stream=True` 及所有 `capabilities` 字段。对象实现 `handle`、`handle_stream` 中的一种或两种，统一通过 `runtime.register` 注册 Actor。

remote wire 版本与 actorx-dist Type 契约分别管理。现在所有注册固定包含 Call、Stream、Started 绑定。原 Call-only Type 新增绑定会改变指纹：已有 Registry 上，provider 和 consumer 须同时选择新 Type，例如 `demo.counter.v2`。重启或 Unregister 不会抹掉已发布契约。原 Stream-capable Type 在 DTO 和绑定不变时保持指纹；仅重命名 Go 代理结构体不要求新 Type。详见[契约参考](../../docs/contract.zh.md)。

开发示例可在全新隔离 NATS 存储上保留原业务 Type 名称。若之前运行已发布 Call-only 契约，使用新的开发存储目录；不清除共享 Registry 来绕过冲突。仅升级 wire 版本不会解决该冲突。

## Go 桥接

所有注册统一使用 `proxyActor` 的固定 Call、Stream、Started 绑定。`Started → actor/create` 成功后才能执行任一种操作。外部 JSON-RPC 信封不进入 actorx-dist 私有 wire；桥接使用独立的 `Invoke → Result` 和 `InvokeStream → Chunk` DTO。它们的契约影响见[Type 迁移](#协议与-type-迁移)。

业务内容在 DTO 字符串字段中保存为 JSON 文本，标记为 `message_type="json"`、`content_type="application/json"`。成功 Call 的 `Result.content` 含 JSON（包括字符串 `"null"`），Failure 为空。Provider 失败使用非空 `Result.Failure` 和空 Content，以 nil Go error 完成 actorx-dist Call；框架失败走 Go error。Stream 通过保留的终态证据维持确定的 provider 错误分类。标记、失败或内容组合非法均为协议失败。

DTO 的 `Invoke.request_id` 是 gateway 内部关联，与 provider RPC ID、actorx-dist 关联分别管理，不提供去重。DTO 包身份及生成的 Bindings 参与 Type 指纹，Go 代理结构体名称不参与。

`Gateway.Stream(ctx, params, yield)` 消费 actorx-dist Stream，回调返回才确认该项。HTTP 使用 `OpenStream` / `ConsumerStream.Run` 和 `Gateway.Control` 关联 consumer 确认与取消；`AcceptEncoded` 保留 provider 消息的实际字节用于窗口计数。流控规则统一见[协议](protocol.zh.md#流控与终态)，消费方式见 [Chat 示例](../examples/chat/README.zh.md)。
