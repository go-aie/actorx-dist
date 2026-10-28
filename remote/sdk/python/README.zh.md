# actorx remote Python SDK

`actorx.remote/v1` 的最小 HTTP Call SDK，提供远端类型注册和 Actor 调用。需要 Python 3.11+，HTTP 依赖为 `httpx>=0.27,<0.29`。包目前用于本地源码安装，尚未公开发布。

从 `actorx-dist/` 安装并验收：

```sh
python3 -m venv /tmp/actorx-remote-demo-venv
/tmp/actorx-remote-demo-venv/bin/python -m pip install -e remote/sdk/python
/tmp/actorx-remote-demo-venv/bin/python -m unittest discover -s remote/sdk/python/tests
```

通过时 unittest 输出 `OK`。测试使用可控的 HTTP transport 和 SSE 字节流，不依赖监听端口或 NATS；独立 gateway/provider/consumer 的运行方式见 [Ping/Pong 示例](../../examples/pingpong/README.zh.md)。

业务入口从 `actorx_remote` 导入：

| API | 行为 |
| --- | --- |
| `Runtime(url, api_key=..., transport="http")` | URL 为 gateway 的 HTTP/HTTPS origin。每个 POST 自动携带 Bearer Key；不跟随重定向、不重试。支持 `async with` |
| `await runtime.register(type, factory, description="")` | 同步无参工厂，返回有 `async def handle(message)` 的对象；注册就绪后返回，此时尚未创建对象 |
| `runtime.addr(type, id)` | 创建本地 `Addr`，不做网络 I/O |
| `await addr.call(message)` | 五秒总预算；首次返回或有效错误中的选代地址先保存到句柄，后续固定该代次 |
| `addr.address` | 地址独立快照；尚未选代时没有 generation |
| `await runtime.wait_closed()` | 等待运行结束；provider 流异常会关闭 Runtime 并向等待者抛出原因 |
| `await runtime.close()` | 幂等关闭 SSE、取消并汇合 SDK 任务、释放对象；上下文退出自动调用 |

消息与返回值必须为 JSON 数据；支持 null、大整数，分数解码为 `decimal.Decimal` 以保留精度，编码同时接受有限的 Python float 和 Decimal。不能传任意 Python 对象。factory 必须短时同步返回，异步 handler 必须配合取消。

类型注解可从同一入口导入 `JSONValue`、`Actor`、`ActorFactory` 和 `Address`。`JSONValue` 表示递归 JSON 数据，包含 Decimal；`Actor` 是结构化 Protocol，无需继承，只需提供 `async def handle(message: JSONValue) -> JSONValue`；`ActorFactory` 是无参同步工厂。`Address` 是 TypedDict，generation 可省略。包包含 `py.typed`，供类型检查器读取这些内联注解。

每个注册最多保存 8 个完整身份的实例和 8 个执行资源槽。同代同 ID 复用对象，不同 ID/generation 分开；同实例 handler 串行，不同实例独立推进。请求排队即计入资源槽，期限覆盖排队、执行和响应 POST。期限先于 handler 启动时终态为 1005；启动后缺少终态为 1004。原预算耗尽后不另开响应预算。超时只结清 RPC，handler 真正退出后才释放锁和资源槽；忽略取消且一直不退出的用户代码也会阻止 close 完成。

`RemoteError.code` 为字符串 kind，`rpc_code` 为整数 RPC code，`data` 保留合法的扩展数据。已知 kind/code 不一致会被拒绝，未知 code 使用通用 RemoteError 和 `code="unknown"`。另提供 AuthenticationError（401）、SessionError（403）、TransportError、ProtocolError、OutcomeUnknownError、ActivationUnknownError、DeadlineExceededError。`asyncio` 取消仍抛 CancelledError。

首次 Call 可能已受理却缺少有效选代证据时，本次返回结果不明，句柄进入不明状态；后续 Call 抛本地 AddressUncertainError。调用者需要显式重新取句柄才可能重新选代。认证、参数拒绝和确定的建连失败不污染句柄；固定地址永不因错误自动换代。错误消息使用固定诊断，不转发用户异常堆栈或服务端原始诊断。

纯 consumer 不建立 SSE。provider 断线不重连；同一 Runtime 可以注册并调用服务。当前没有 Send、Stream、WS 或远端取消 API。

协议与 wire 限制见 [Remote 协议](../../docs/protocol.zh.md)，地址、并发和清理语义见[运行模型与生命周期](../../docs/runtime.zh.md)。
