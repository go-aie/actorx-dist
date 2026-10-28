# Remote 协议

[English](protocol.md) | 中文

本文说明已实现的 `actorx.remote/v1` Call 契约。[运行模型与生命周期](runtime.zh.md)说明执行和清理语义。wire 结构及校验见 [protocol](../protocol/messages.go) 和 [rpc.go](../protocol/rpc.go)，HTTP 绑定见 [handler.go](../http/handler.go)。

## 信封与数据

消息采用 JSON-RPC 2.0：请求包含 `jsonrpc`、`method`、命名对象 `params` 和 `id`；响应携带相同 `id`，且 `result`/`error` 必须恰好出现一个，包括 result 为 null 时。方法按完整的斜杠分隔名称匹配，不提供点号别名。

Gateway/SDK 生成非空字符串 ID。解析器也接受数字和 null ID，并原样回显，不损失精度。只有缺少 `id` 才是通知；`id:null` 仍需响应。ID 只关联一段通信中的请求，不提供业务去重。

JSON 必须为 UTF-8，不允许重复键、NaN/Infinity。已知信封字段严格校验，未知信封字段忽略。未知方法参数返回 -32602；业务 `input`/`value` 内部结构由应用定义。下表要求的这两个字段必须存在，但可以为 null。边界保留业务 JSON 和数字 ID 的精度。

## 身份与方法

地址采用 dist 快照格式：

```json
{"version":1,"type":"demo.ping","id":"session-42","generation":"<dist generation>"}
```

首次 `actor/call` 可以省略 `generation`。选代后，SDK 将该 Addr 固定在返回的 generation。`actor/create`、`actor/invoke` 必须使用完整地址。generation 由 dist 分配，是 16 字节数据的规范无填充 base64url 编码。地址版本 1、JSON-RPC 版本 2.0 和 `actorx.remote/v1` 分别描述不同层次。

| 方法及方向 | params | 成功 result |
| --- | --- | --- |
| `runtime/register`，provider → gateway | `type`，可选 `description`（默认空串） | `protocol`、`type`、`session_token` |
| `runtime/services`，consumer → gateway | 空对象；可省略 params | `services`：元素为 `type`/`description`/`protocol` 的数组 |
| `actor/call`，consumer → gateway | `address`、`input`，可选 `deadline_unix_ms` | `address`、`value` |
| `actor/create`，gateway → provider | 固定 `address`、`deadline_unix_ms` | 相同 `address` |
| `actor/invoke`，gateway → provider | 固定 `address`、`input`、`deadline_unix_ms` | 相同 `address`、`value` |

期限为 UTC Unix 毫秒整数。省略与显式零值不同：零值已过期。注册只证明工厂已就绪，不创建对象。服务列表是快照，不保证后续可用。Call 结果证明被调用 handler 已返回，不证明附加后台工作已结束。

方向不匹配的方法返回 -32601。方向不代表凭据角色，同一个 Runtime 可以注册并调用。provider 响应按本次注册和出站 RPC ID 关联；成功响应的地址、错误中存在的 `error.data.address` 均须与 pending 完整地址匹配。结果最多消费一次，错误或迟到响应不能消费合法等待项。

## HTTP 与 SSE

所有通信使用 `POST /runtime/rpc`，携带以下 Header：

```http
Authorization: Bearer <api-key>
Content-Type: application/json
X-Actorx-Remote-Protocol: actorx.remote/v1
```

协议 Header 缺省按 v1 处理，显式不支持的值在分派前返回 HTTP 400。普通调用接受 `application/json`。注册使用 `Accept: text/event-stream, application/json`，同时接收成功流和 JSON 错误。

| 入站内容 | 响应 |
| --- | --- |
| 单条有 ID 且接受 SSE 的 `runtime/register` 请求 | HTTP 200 SSE；首帧为注册 RPC result，随后仅发送本次注册的 create/invoke 请求 |
| Call、services 或支持的请求 batch | HTTP 200 JSON RPC result/error 或响应数组 |
| 通知或全通知 batch | 分派后 HTTP 204；无 RPC 响应，不作为执行完成 ACK |
| 带会话 Header 的单条 provider 响应 | 校验交接后 HTTP 204，不再产生 RPC 响应 |

注册不满足单请求/SSE 条件时不创建注册：有 ID 返回 1008，通知不返回 RPC 响应。建立流前的注册失败返回普通 HTTP 200 JSON RPC error。无法接受可用响应格式时返回 HTTP 406。

每个 SSE 帧为 `data: <完整单行 JSON-RPC 消息>` 加空行，立即 Flush。SSE event 名和 ID 不替代 RPC method/id。EOF 只代表连接结束，不证明 RPC 成功。当前无恢复游标或自动重连；gateway 复用已建立的 SSE，不主动连接 provider 的监听地址。

provider 响应 POST 除 API Key 外，还携带 `X-Actorx-Remote-Session: <session_token>`。令牌取自注册首帧，不属于 JSON-RPC 信封字段，仅用于 provider 响应，不随普通 consumer 调用发送。带该 Header 的请求/通知返回 400；缺少它的响应返回 403。provider 响应不能批量提交，gateway 也不向 provider 发送 batch。

请求 batch 最多八项，顺序执行并共享一个 HTTP 预算，不提供原子性。空 batch、非法元素遵循 JSON-RPC 错误规则；通知不生成响应项。batch 中注册永不安装服务（有 ID 的项返回 1008）。请求 batch 内嵌 provider 响应会在分派前拒绝整个文档。Python SDK 不暴露 batch API。

## Call 报文流程

以下为报文示意，generation、token 和期限占位值须替换为运行时值。JSON 代码块中每一行均为独立消息。

provider 注册，SSE 首个 `data` 消息为注册结果：

```json
{"jsonrpc":"2.0","id":"p:1","method":"runtime/register","params":{"type":"demo.ping"}}
{"jsonrpc":"2.0","id":"p:1","result":{"protocol":"actorx.remote/v1","type":"demo.ping","session_token":"<token>"}}
```

consumer 不带 generation 发起 Call。gateway 先经 SSE 创建对象，等待匹配的 create 结果，再执行 invoke：

```json
{"jsonrpc":"2.0","id":"c:1","method":"actor/call","params":{"address":{"version":1,"type":"demo.ping","id":"session-42"},"input":{"text":"ping"}}}
{"jsonrpc":"2.0","id":"g:create-1","method":"actor/create","params":{"address":{"version":1,"type":"demo.ping","id":"session-42","generation":"<generation>"},"deadline_unix_ms":1890000000000}}
{"jsonrpc":"2.0","id":"g:create-1","result":{"address":{"version":1,"type":"demo.ping","id":"session-42","generation":"<generation>"}}}
{"jsonrpc":"2.0","id":"g:invoke-1","method":"actor/invoke","params":{"address":{"version":1,"type":"demo.ping","id":"session-42","generation":"<generation>"},"input":{"text":"ping"},"deadline_unix_ms":1890000000000}}
{"jsonrpc":"2.0","id":"g:invoke-1","result":{"address":{"version":1,"type":"demo.ping","id":"session-42","generation":"<generation>"},"value":{"text":"pong"}}}
{"jsonrpc":"2.0","id":"c:1","result":{"address":{"version":1,"type":"demo.ping","id":"session-42","generation":"<generation>"},"value":{"text":"pong"}}}
```

create/invoke 响应通过鉴权的响应 POST 提交，各自交接后返回 204。最终 `c:1` 响应在 consumer 原 HTTP 请求上返回。consumer 与 provider 段的 RPC ID 相互独立；provider 的 204 不证明 consumer 已收到结果。同一 Addr 的后续调用携带选定的 generation。

## 鉴权与传输错误

每个 POST 都在读取 body、查找会话或执行 Actor 工作前鉴权。一个共享静态 Key 授予服务接入，不划分 consumer/provider 角色、Type ACL 或租户。session token 关联一次注册的响应，不是另一种接入凭据或幂等键。

缺少 Key 返回 401 和 `WWW-Authenticate: Bearer realm="actorx-remote"`；错误 Key 增加 `error="invalid_token"`。重复或歧义 Authorization Header 被拒绝，Key 采用恒定时间比较。通过受信环境配置至少 32 随机字节生成的 Key，凭据/令牌不放 URL 或日志。loopback 演示之外使用 HTTPS 并校验证书，TLS 终止后的链路也须受保护。SDK 不跟随重定向。

| HTTP 状态 | 传输拒绝 |
| --- | --- |
| 400 | 协议或会话 Header 非法/歧义；provider 响应损坏 |
| 401 | API Key 缺失、错误或歧义 |
| 403 | 会话不存在/已关闭，或响应缺少会话 Header |
| 405 | 非 POST 方法 |
| 406 | 响应格式不可接受 |
| 409 | provider RPC ID 未知/已结清，或地址不匹配 |
| 413 | body、项数或编码响应超限 |
| 415 | Content-Type 不是 application/json |

这些拒绝使用 HTTP 状态，不生成 RPC 错误。非法 JSON 请求、无效 RPC 信封和业务失败使用 RPC error。SSE 无法设置写期限时，建立流也可能返回 HTTP 500。业务失败仍为 HTTP 200，不改成 502/504。

## RPC 错误

标准错误为 -32700 `parse_error`、-32600 `invalid_request`、-32601 `method_not_found`、-32602 `invalid_params`、-32603 `internal_error`。应用错误使用正整数：

| code | `error.data.kind` | 含义 |
| --- | --- | --- |
| 1000 | `remote_error` | provider 工厂/handler 异常 |
| 1001 | `full` | 明确容量拒绝 |
| 1002 | `service_unavailable` | 无已注册服务，尚未投递业务工作 |
| 1003 | `activation_unknown` | 激活/选代证据不明 |
| 1004 | `outcome_unknown` | 可能已执行，缺少有效终态 |
| 1005 | `deadline_exceeded` | 确定的期限失败 |
| 1006 | `stopped` | 确定的停止/关闭失败 |
| 1007 | `registration_conflict` | 本地同 Type 已注册或正在撤销 |
| 1008 | `transport_mode` | 不满足注册传输前置条件 |
| 1009 | `canceled` | 不含不明分类的取消 |
| 1010 | `owner_unavailable` | dist owner 不可达，不自动接管 |
| 1011 | `contract` | dist DTO/Type 契约不匹配 |
| 1012 | `protocol` | 内部协议/接线失败 |

错误回显请求 ID，无法确定时为 null。Gateway 应用错误含 `data.kind`，已知时含 `data.address`；地址不放在 RPC 顶层：

```json
{"jsonrpc":"2.0","id":"c:1","error":{"code":1004,"message":"Provider outcome is unknown","data":{"kind":"outcome_unknown","address":{"version":1,"type":"demo.ping","id":"session-42","generation":"<generation>"}}}}
```

经校验的 provider 错误 1000/1001/1004/1005/1009 保留分类；其他合法应用错误归一为 1000，已知出站方法返回标准 RPC 错误则转为 1012。已知 kind/code 必须一致。语言异常类型、任意 provider data 和堆栈不穿过 dist；错误消息为最多 512 UTF-8 字节的安全诊断。

已确认的 provider 失败证据优先于稍后的停止/取消。缺少确定终态时，不明分类优先：outcome_unknown → activation_unknown → 具体框架错误。仅凭超时或断线不能证明 handler 未执行。任何错误都不触发透明重试，详见[地址与期限规则](runtime.zh.md)。

## 字节限制与兼容

请求 body 和单条编码消息（含信封）上限 8 KiB。batch 入站仍限 8 KiB、最多八项，编码响应总上限 64 KiB。JSON 嵌套最多 64 层。SSE 每帧写期限五秒，注册长连接本身不受五秒寿命限制。

外部 JSON-RPC 信封不进入 dist 私有 wire。业务数据使用稳定的 [Invoke/Result/Failure DTO](../protocol/messages.go)，`message_type="json"`、`content_type="application/json"`。`Invoke.content` 是 JSON 文本；成功的 `Result.content` 是 JSON 文本（含字符串 `"null"`），Failure 为空。provider 失败使用非空 Failure 和空 Content，经 dist 以 nil Go error 完成；框架失败走 Go error 路径。标记、失败或内容组合非法均为协议失败。

`Invoke.request_id` 是 gateway 生成的内部关联，与 provider RPC ID、dist 私有关联分别管理，不提供去重。DTO 包身份和生成的 Bindings 参与 dist Type 契约，修改须规划契约升级。执行容量见[资源限制](runtime.zh.md)。该契约尚未实现 Stream、Send、WebSocket 或远端取消。
