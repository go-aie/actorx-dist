# actorx remote Python SDK

[English](README.md) | 中文

`actorx.remote/v1` 的 HTTP Call/Stream SDK，提供统一的远端 Actor 注册、寻址与调用。需要 Python 3.11+，HTTP 依赖为 `httpx>=0.27,<0.29`。包目前用于本地源码安装，尚未公开发布。

## 安装与运行

从 `actorx-dist/` 安装并检查：

```sh
python3 -m venv /tmp/actorx-remote-demo-venv
/tmp/actorx-remote-demo-venv/bin/python -m pip install -e remote/sdk/python
/tmp/actorx-remote-demo-venv/bin/python -m unittest discover -s remote/sdk/python/tests
```

通过时 unittest 输出 `OK`。这些测试使用可控的 HTTP transport 和 SSE 字节流，不依赖监听端口或 NATS。独立 gateway/provider/consumer 的完整运行命令及预期输出见 [Counter](../../examples/counter/README.zh.md)（Call）与 [Chat](../../examples/chat/README.zh.md)（Stream）。

## 一个 Actor，两种操作

业务入口从 `actorx_remote` 导入。Actor 可实现 `handle`、`handle_stream` 中的一种或两种；将类传给 `runtime.register` 即可注册，如下例所示。以下代码放在已建立的 Runtime 中；provider（提供方）和 consumer（调用方）可位于独立进程：

```python
from actorx_remote import Runtime

class ChatActor:
    def __init__(self):
        self.turns = 0

    async def handle(self, message):
        self.turns += 1
        return {"text": "complete answer", "turn": self.turns}

    async def handle_stream(self, message):
        self.turns += 1
        for text in ("first fragment", "second fragment"):
            yield {"text": text, "turn": self.turns}

# Provider
await runtime.register("demo.chat", ChatActor)

# Consumer
actor = runtime.addr("demo.chat", "session-42")
answer = await actor.call({"text": "hello"})
async with actor.stream({"text": "hello"}, timeout=30) as chunks:
    async for chunk in chunks:
        print(chunk["text"], chunk["turn"])
```

首次 Call 返回 `turn: 1`，后续 Stream 的两项均带 `turn: 2`：两种操作复用同一代实例、锁和执行资源槽。不同 ID/generation 创建独立对象。Call 等待单个 JSON 值；Stream 消费零到多个 JSON 项及明确终态，SDK 不按返回值自动改换操作。

注册只建立会话，不创建 Actor 实例或探测对象方法。首次 Call 或 Stream 访问新 ID 时，SDK 才创建对应对象。真实对象必须至少有一个可调用的 `handle` / `handle_stream`；创建失败或两者均不可调用时返回 1000。

执行时在实例锁内检查对应方法。`handle(message)` 必须返回可 await 的结果并产出一个 JSON 值；`handle_stream(message)` 必须直接返回异步迭代器，支持异步生成器和自定义异步迭代器。已调用方法但返回形态非法、输出非 JSON 或业务抛异常时返回 1000。

## API 与类型

| API | 行为 |
| --- | --- |
| `Runtime(url, api_key=..., transport="http")` | URL 为 gateway 的 HTTP/HTTPS origin。每个 POST 自动携带 Bearer Key 和 v1 协议 Header；不跟随重定向、不重试。支持 `async with` |
| `await runtime.register(type, factory, description="")` | 注册 Actor；`factory` 用于创建对象。注册就绪后返回，此时尚未创建实例。没有 `stream` 参数 |
| `runtime.addr(type, id)` | 创建本地 `Addr`，不做网络 I/O |
| `await addr.call(message)` | 五秒总预算；首次返回或有效错误中的选代地址保存到句柄，后续固定该代次 |
| `addr.stream(message, timeout=5)` | 返回流 context，期限接受 (0, 60] 秒；通过 `async with` 和 `async for` 消费 |
| `addr.address` | 地址独立快照；尚未选代时没有 generation |
| `await runtime.wait_closed()` | 等待运行结束；provider 流异常会关闭 Runtime 并向等待者抛出原因 |
| `await runtime.close()` | 幂等关闭 SSE、取消并汇合 SDK 任务、释放对象；上下文退出自动调用 |

对 provider 作者，注册的第二个参数 `factory` 是用于创建 Actor 对象的无参同步可调用对象。可直接传入能无参构造的类，如 `runtime.register("demo.chat", ChatActor)`；需要自定义创建时，也可传入函数或闭包，如 `runtime.register("demo.configured", lambda: ChatActor())`。SDK 仅在需要实例时调用它，它必须短时同步返回。这是 Python SDK 的对象创建接口，远程协议不传输工厂。

消息与返回值必须为 JSON 数据；支持 null、大整数，分数解码为 `decimal.Decimal` 以保留精度，编码同时接受有限的 Python float 和 Decimal。不能传任意 Python 对象。异步 handler 必须配合取消。

同一入口导出 `JSONValue`、`Actor`、`StreamActor`、`ActorFactory` 和 `Address`。`JSONValue` 表示递归 JSON 数据，包含 Decimal；`Actor` / `StreamActor` 是静态类型辅助 Protocol，分别描述返回 `Awaitable[JSONValue]` 的 `handle` 和返回 `AsyncIterator[JSONValue]` 的 `handle_stream`。无需继承，不划分注册或对象生命周期。`ActorFactory` 是无参同步工厂；`Address` 是 TypedDict，generation 可省略。包包含 `py.typed`，供类型检查器读取内联注解。

每个注册最多保存 8 个完整身份的实例和 8 个执行资源槽。同实例 handler 串行，不同实例独立推进。请求排队即计入资源槽，期限覆盖排队、执行和响应 POST。期限先于 handler 启动时终态为 1005；启动后缺少终态为 1004。原预算耗尽后不另开响应预算。超时只结清 RPC，handler 真正退出后才释放锁和资源槽；忽略取消且一直不退出的用户代码也会阻止 close 完成。

每个注册另有 8 条 Stream 账本的独立上限，包含终态后仍待 ack 的流。账本满时，新 Stream 返回 1001/full；Call 仍可使用空闲执行槽。Ack、原期限或注册关闭会回收保留的账本。

## 查询目录

```python
report = await runtime.actors()
print(report["actors"])
```

查询分布式 Runtime 在采集窗口内观察到的兼容 Remote 类型，无需范围参数。同 Type 去重，来源描述不一致时为空。结果中的 `actors` 是类型声明，不是运行实例。响应只包含 `actors`。发现采用有限采集窗口，不保证观察到所有节点；底层快照或报告因限额截断时返回 1001/full，不返回部分目录。

查询总预算最多五秒，Runtime 关闭会取消并汇合查询；失败不返回部分目录，不自动重试。完整响应限 8 KiB，超出时 Gateway 拒绝响应。跨 Gateway 调用入口尚待补齐，当前目录可见不代表任意入口都可 Call/Stream 该类型。详见 [目录协议](../../docs/protocol.zh.md#runtimeactors列出已注册的-actor-类型)。

## 错误与固定地址

`RemoteError.code` 为字符串 kind，`rpc_code` 为整数 RPC code，`data` 保留合法扩展数据。已知 kind/code 不一致会被拒绝，未知 code 使用通用 `RemoteError` 和 `code="unknown"`。另提供 `AuthenticationError`（401）、`SessionError`（403）、`TransportError`、`ProtocolError`、`OutcomeUnknownError`、`ActivationUnknownError`、`DeadlineExceededError` 和 `UnsupportedOperationError`。`asyncio` 取消仍抛 `CancelledError`。

请求的方法缺失或不可调用时抛 `UnsupportedOperationError(RemoteError)`，其 `rpc_code == 1013`、`code == "unsupported_operation"`。错误带选定的完整地址；Stream 必须为零项且 `final_seq: 0`。这证明该业务方法没有进入，但此前仍可能构造对象。SDK 保存 generation，原 Addr 可继续执行已支持的操作。例如在 Counter provider 运行时：

```python
from actorx_remote import UnsupportedOperationError

actor = runtime.addr("demo.counter", "orders-42")
try:
    async with actor.stream({"n": 5}) as chunks:
        async for chunk in chunks:
            print(chunk)
except UnsupportedOperationError as exc:
    print(exc.rpc_code, actor.address["generation"])

count = await actor.call({"n": 5})  # 原 Addr、同一代实例
```

缺少地址或矛盾的 Stream 终态不能作为可信 1013，按 1004 处理。1013 与实例创建或业务失败 1000、未知 JSON-RPC 方法 -32601 分别处理。

首次 Call 或 Stream 可能已受理却缺少有效选代证据时，本次返回结果不明，句柄进入不明状态；后续操作抛本地 `AddressUncertainError`。调用者需要显式重新取句柄才可能重新选代，这不会对之前的工作去重。认证、参数拒绝和确定的建连失败不污染句柄；固定地址永不因错误自动换代。错误消息使用固定诊断，不转发用户异常堆栈或服务端原始诊断。

## 流的消费与关闭

未完成的 Stream 关闭后，等待中和后续的迭代都会抛出 `rpc_code == 1009` 的 `RemoteError`；正常耗尽的流在关闭后仍正常结束。合法成功终态已验证且全部数据已消费时，最后 ack 的失败或超时不会改写成功结果。

处理完当前项、请求下一项时，SDK 才发送消费确认。默认总期限五秒，`timeout` 接受 (0, 60] 秒，不因收到 chunk 延长。正常耗尽表示已收到合法终态并处理完全部项；空流也必须有明确终态。部分输出后失败仍抛异常，已处理内容不回滚。

提前 `break` 必须退出 context 或显式 `await chunks.aclose()`，关闭发送取消并回收任务；Runtime 关闭也负责回收。取消不停止共享 Actor，不证明外部副作用已结束；SDK 在 handler 实际退出前保留实例锁和执行槽。不要只依赖垃圾回收结束流。

纯 Call consumer 不建立 SSE；每个 Stream consumer 建立独立 SSE。provider 断线不重连；同一 Runtime 可以注册并调用 Actor。当前没有 Send、WS 或 Call 的远端取消 API。Stream 支持按请求取消。

## 版本迁移与参考

当前仍处于早期开发，gateway 与 SDK 保留 `actorx.remote/v1`，须配套使用。SDK 自动发送版本 Header 并校验注册结果，作者无需配置协议或能力开关。早期代码更新时删除 `stream=True` 和 `capabilities`；版本 Header 仍须显式携带 v1。旧 Call-only Type 因新增 Stream 绑定须选择新 Type，旧 Stream-capable Type 在 DTO/绑定不变时保持契约。详细条件及开发存储处理见[协议与 Type 迁移](../../docs/runtime.zh.md#协议与-type-迁移)。

- [Counter](../../examples/counter/README.zh.md)与 [Chat](../../examples/chat/README.zh.md)：独立进程示例。
- [运行模型与生命周期](../../docs/runtime.zh.md)：地址、并发与清理语义。
- [Remote 协议](../../docs/protocol.zh.md)：Call/Stream 消息过程、wire 限制、两段 credit 和窗口。

- [Go SDK](../go/README.zh.md)：通过相同 gateway 与 Python provider/consumer 互通。
