# Remote 运行模型与生命周期

[English](runtime.md) | 中文

Remote 在 dist 之上提供跨语言接入。[协议文档](protocol.zh.md)定义 wire 契约，[Ping/Pong 示例](../examples/pingpong/README.zh.md)提供可运行命令。

## 组件与激活

```text
consumer SDK → HTTP actor/call → Gateway → dist ProxyActor
                                              ↓
provider 对象 ← provider SDK ← 注册 SSE create/invoke
                       ↓
                HTTP 响应 POST → Gateway → 原 Call 结果
```

| 组件 | 职责 |
| --- | --- |
| actorx | mailbox、Deferred Reply、自有任务与本地生命周期 |
| dist | Type/ID 寻址、选代、代理唯一归属与 Call |
| remote | Gateway、注册、provider 会话、ProxyActor 与响应关联 |
| remote/http | 鉴权、路由、JSON 转换、SSE 和连接/写期限 |
| Python SDK | 工厂、业务对象、串行 handler、地址状态与 HTTP/SSE 传输 |

gateway 借用已有的一个 `*actorxdist.Runtime`，外部程序无需 NATS 客户端或监听端口。HTTP/SSE 是接入适配，不是新增的 actorx Channel 实现。SDK Runtime 管理外部连接及 Python 对象，actorxdist.Runtime 管理代理归属；SDK 不 claim 归属，也不生成 dist generation。

`register(type, factory)` 安装创建能力并建立 provider SSE，在校验注册结果后返回，此时尚未创建业务对象。`addr(type, id)` 只创建本地句柄。首次 Call 由 dist 查找或激活 ProxyActor；代理在 `Started` 中取得完整 Self 地址并等待 `actor/create` 成功，之后才能执行 `actor/invoke`。已有就绪代理则省略创建。

SDK 按本次注册和完整地址（Type/ID/generation）保存对象。同一身份的重复 create 复用对象，并响应新的 RPC ID；不同 ID 或代次使用独立对象。工厂是短时同步无参函数，返回具有 `async def handle(message)` 的对象。同实例 handler 串行执行，不同实例独立推进。

## 地址与错误

SDK 在返回值或抛 RPC 异常前，先保存 `result.address` 或 `error.data.address` 中有效的已选代地址，因此业务失败也可能固定 Addr 的 generation。固定后拒绝不同代次，不会隐式向新代重试。

首次 Call 可能已受理却缺少有效选代证据时，句柄进入不明状态。本次报告不明分类（或保留调用者取消），后续调用在本地抛 `AddressUncertainError`。重新取得句柄是调用者的显式决定，不会对之前的工作去重。鉴权/参数拒绝和确定的建连失败不污染句柄。

Python `RemoteError.code` 为字符串 kind，`rpc_code` 为 wire 整数。已知 kind/code 不一致属于协议错误；未知合法 wire code 使用通用 RemoteError 和 `code="unknown"`。HTTP 鉴权/会话错误使用 `AuthenticationError`/`SessionError`，传输和损坏响应分别使用对应异常。完整 API 见 [SDK 指南](../sdk/python/README.zh.md)。

## 期限与并发

SDK Call 的五秒总预算覆盖句柄锁、HTTP 发送与响应。本地使用单调时钟，wire 携带绝对 UTC `deadline_unix_ms`。gateway 取 HTTP 入口五秒预算、请求指定期限、父 context 期限的最早者；已过期请求在投递前失败。后续阶段沿用剩余期限，不重新开启 Call 预算。

创建属于 dist 共享激活，代理 `Started` 的 create 等待上限为三秒或更早的生命周期期限。单个 consumer 取消只限制自己等待就绪的时间，不直接取消共享激活。注册成功后，五秒注册等待结束，SSE 转为 Runtime 持有的长连接。本机示例尚未验证跨主机时钟假设。

provider 执行规则：

1. 等待实例锁、handler 执行、响应 POST 共用传入期限，请求排队即占执行槽。执行前容量拒绝为 1001。
2. handler 启动前过期为 1005，该请求之后也不能执行；明确的启动前取消为 1009。
3. 启动后超时/取消且缺少有效终态为 1004。SDK 请求任务取消并结清 RPC，但直到 handler 实际退出才释放实例锁和执行槽。
4. 最终响应最多提交一次，且限于原预算内；不为报告过期开新预算或重试。结清后 handler 才返回时，只释放资源，不发送第二份结果。

SSE 读取和响应 I/O 不受实例锁阻塞。不配合取消的 handler 会继续占用实例，也可能阻止 SDK close 完成。客户端返回、await 被取消、provider EOF 均不证明外部副作用已停止。当前没有远端 cancel 方法。

Go 代理每实例只受理一个活动业务 Call，重叠调用返回 full；SDK 串行锁还保护 RPC 已结清但任务仍未退出的情况。同一个 Python Addr 会串行自己的调用，但不同句柄之间不共享客户端锁。

## 资源限制

| 资源 | 当前上限 |
| --- | --- |
| Gateway 活动 Call | 整个 Gateway 共 8 个 |
| 每注册出站请求 | 队列 8 项 |
| 每注册 pending create / invoke | 各 8 项 |
| 每注册保留的激活记录 | 8 条 |
| 每实例代理活动业务 Call | 1 个 |
| 每注册 Python 对象 / 执行槽 | 各 8 个 |

执行槽包括排队请求和超时后仍未退出的 handler，仅结清 RPC 不能绕过容量。dist 有独立的[执行和传输限制](../../docs/runtime.zh.md)，默认实例容量 64、payload 上限 64 KiB；这些与 remote 完整消息的 [wire 限制](protocol.zh.md)分别校验。当前 remote 常量不提供公共调节选项。

已校验的创建失败按注册及完整地址保留到清理和相关外层 Call 结束，避免稍后的停止/取消抹掉确定失败。执行派发/终态证据按 gateway 请求 ID 保留到对应 Call 结清。这些记录用于错误分类，成功值仍经普通 dist Result DTO 返回。

## Go 接入与所有权

Go 代码属于现有 dist 模块。[Gateway](../gateway.go)接入已有 Runtime；[remotehttp.NewHandler](../http/handler.go)返回 `net/http.Handler`，不自行启动服务。HTTP 包导入名为 `remotehttp`。

| API | 所有权与完成语义 |
| --- | --- |
| `remote.NewGateway(rt)` | 借用 Runtime，由创建者负责最终关闭 |
| `Gateway.Register(ctx, params)` | ctx 限制安装，不持有后续 provider 连接寿命 |
| `Registration.Info()` | 不可变注册数据，含 session token |
| `Registration.Events()` | 一个传输读取者；session 结束受理时关闭 |
| `Registration.Done()` | dist Unregister 及所有已受理外层 Call 清理结束后关闭 |
| `Registration.Close(ctx)` | 幂等关闭 session 并等待所属清理 |
| `Gateway.Accept(ctx, token, response)` | 校验/关联 provider 响应，不执行用户 handler |
| `Gateway.Call(ctx, params)` | 通过 dist Call，错误时也保留选代地址 |
| `Gateway.Services()` | 就绪本地注册的独立快照 |
| `Gateway.Close(ctx)` | 拒绝新工作、等待所属注册，不关闭借用 Runtime |
| `remotehttp.NewHandler(gateway, options)` | 要求 `Options{APIKey: key}`，不接管 Gateway/Runtime 所有权 |

Register 在开始安装后，可能同时返回非空句柄和错误；调用方仍须 Close 并等待该句柄。关闭超时只结束本次等待，清理责任仍保留，可再次等待。

SSE 要求 ResponseController 写期限与 Flush；包装 ResponseWriter 的中间件应实现 `Unwrap`。使用逐帧期限，短的全局 WriteTimeout 会中断健康注册。[gateway 入口](../examples/pingpong/gateway/main.go)展示装配与独立清理 context。

## 断线与关闭

provider HTTP 请求取消、写失败或 SSE EOF 会发起注册关闭：撤目录、拒绝新工作、唤醒 pending、Unregister 所属代理。token 随之不可用；清理期间本地同 Type 注册仍冲突。SDK 在 provider 流失败后关闭，不自动重新注册。

按以下顺序关闭，各阶段使用独立、有界且尚未取消的 context：

1. `Gateway.Close`：结束注册和 provider SSE，等待已受理工作。
2. `http.Server.Shutdown`：等待 HTTP handler。
3. 创建者调用 `actorxdist.Runtime.Close`：关闭借用的分布式 Runtime。

Python `Runtime.close()` 关闭连接、取消并汇合 SDK 自有任务，然后释放对象引用。取消某个 close 等待者不丢失清理责任。当前不提供 Python 状态持久化、provider 接管、自动重放或租约恢复。静态 API Key 不热轮换或自动到期，撤销还需终止现有注册连接。

独立进程示例证明本机源码链路，不代表已验证公开发行包、跨平台或生产恢复保证。
