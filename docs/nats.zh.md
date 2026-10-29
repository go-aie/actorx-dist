# NATS 技术参考

[English](nats.md) | 中文

本文说明 actorx-dist 如何使用 NATS 实现跨进程调用、按需激活和归属协调，并提供 subject、存储名称及权限参考。连接与认证配置见[部署指南](deployment.zh.md)，帧格式见 [wire 契约](contract.zh.md)，存储算法见 [Registry 协议](registry.zh.md)。

## NATS 如何支撑分布式 Actor

actorx-dist 使用 **Core NATS 传递请求和回复**，使用 **JetStream KV 保存协调元数据**。Core NATS 将消息送到提供该 Actor 的进程；接收端再通过 Actorx mailbox 串行执行业务 handler。KV 保存契约、当前归属和退出记录，供不同进程判断某个 Type/ID 对应哪一次激活，以及它是否已经完整退出。

以一次 Call 为例，通信与协调按以下顺序配合：

1. 调用方读取或命中缓存中的 Type 契约，核对消息和结果类型，再查询当前归属。
2. 已有激活由原 Factory 确认；需要新建时，通过 Factory queue group 选择一个 Runtime。该 Runtime 检查容量，通过 KV 条件写入取得归属，再创建实例并发布 READY 状态。
3. 调用方固定本次激活的代次（generation），向 Actor 业务 subject 发送请求，接收端核对身份后投递到本地 mailbox。
4. handler 完成后，结果经调用方的回复 inbox 返回。后续调用复用已运行实例，直到该代退出。

不同功能使用的能力如下：

| actorx-dist 功能 | 如何使用 NATS |
| --- | --- |
| Send / Call | Core NATS 投递请求，经回复 inbox 返回受理确认或最终结果 |
| 按需激活与唯一归属 | queue group 分发激活请求，JetStream KV 条件创建/更新确定同一 Type/ID 的归属 |
| 契约注册与核对 | KV 保存每个 Type 的不可变契约；注册时比较完整声明，调用前核对操作、类型和模式 |
| Stream 与取消 | Core NATS 传递分片、终止和取消帧；消费方通过额度 inbox 归还窗口额度 |
| Stop / Wait | 专属 Factory 控制入口传递停止请求；KV 保存固定代次的退出证据，供 Wait 读取 |
| 显式恢复与元数据维护 | KV 条件更新/删除释放已隔离进程的归属；JetStream 按 subject 和序号上限回收符合条件的记录 |

理解这些能力时，需要区分三个边界：

- **分发与归属：** queue group 为一次激活请求选择处理方；唯一归属由 KV 条件写入保证。按 revision 更新（CAS）要求记录仍是已观察的版本，才能推进状态。
- **传输与持久状态：** 业务消息不写入 JetStream，Actor 业务状态保存在提供方进程内存中。断线不缓冲或重放业务请求；重连校验后可恢复受理，但不自动接管失联 Actor，也不恢复旧流。
- **流式传输与消费确认：** Stream 的序号、终止和窗口规则由 actorx-dist 协议实现。额度通过 Core NATS 返回，业务流不使用 JetStream consumer 的 ACK/流控。

Remote provider/consumer 通过 HTTP/SSE 与 Gateway 通信；Gateway 内部的 actorx-dist Runtime 使用上述 NATS 路径。完整时序见[激活与业务路径](architecture.zh.md#激活与业务路径)，完成、取消与结果未知的语义见[调用语义](runtime.zh.md)。

## Namespace 与命名规则

`WithNamespace` 配置单个 Runtime 的命名空间，默认值为 `default`，统一影响消息 subject、Factory queue group 和三个 KV bucket。同一 NATS account、相同 namespace 的 Runtime 共享路由与 Registry；需要协作的 Runtime、Gateway 和恢复工具必须使用相同 namespace。Remote SDK 通过 Gateway 使用其 Runtime 的 namespace。

下方统一使用 `<namespace>` 表示配置值。例如设为 `prod`，消息前缀为 `actorxdist.prod`，契约 bucket 为 `actorxdist_prod_types_v1`。Actor Type/ID、随机 `_INBOX.*` 与 JetStream API 前缀不随 namespace 改变。API 用法和名称校验见 [Namespace 配置](runtime.zh.md#namespace-与部署隔离)；恢复工具使用 `dist-recover -namespace prod ...`，详见[显式恢复](deployment.zh.md#显式恢复与存储修复)。

## Subject 参考

### 消息与队列组

`T`、`I` 分别为 Type、ID 原始字节的无填充 base64url 编码。例如 `counter` → `Y291bnRlcg`，`orders-42` → `b3JkZXJzLTQy`。`F` 是注册的 Factory token，`R` 是请求关联 ID，`N` 是 Runtime inbox 的随机 token。随机 token 已编码，不二次编码；规范见 [wire 身份规则](contract.zh.md#名称与规范身份)。

| 发布 subject | 用途 | 接收方式 |
| --- | --- | --- |
| `actorxdist.<namespace>.factory.<T>` | 按 Type 请求激活 | 提供该 Type 的 Runtime 使用 queue group 订阅 |
| `actorxdist.<namespace>.factory.<T>.<F>.control.confirm` | 确认选定归属和状态 | 指定注册普通订阅 `actorxdist.<namespace>.factory.<T>.<F>.control.*` |
| `actorxdist.<namespace>.factory.<T>.<F>.control.stop` | 停止指定代次 | 与 confirm 共用上述控制订阅 |
| `actorxdist.<namespace>.actor.<T>.<I>` | Send/Call/Stream 请求及 Call/Stream 取消 | 当前有效实例普通订阅 |
| `_INBOX.<N>.<R>` | 激活与控制回复、业务回复、Stream 分片和终止 | Runtime 普通订阅 `_INBOX.<N>.*` |
| `_INBOX.<N>.control.<R>` | Stream 消费后归还窗口额度 | Runtime 普通订阅 `_INBOX.<N>.control.*` |
| `actorxdist.<namespace>.discovery.v1.query` | 内部发现广播 | 每个 Runtime 普通订阅，无 queue group |
| `actorxdist.<namespace>.discovery.v1.reply.<RuntimeID>` | 单节点快照或 full/unavailable 回复 | 发起 Runtime 一个常驻普通订阅 |

Factory queue group 名为 `actorxdist.<namespace>.factory.<T>.workers`，不是发布目的 subject。控制入口与业务入口不使用 queue group。业务操作名放在帧内，不为每个 handler 创建 subject；取消使用原业务 subject。Runtime 延迟建立并共用回复和额度订阅，不按调用或流新增应用订阅。

发现的两个订阅在初始化时安装并 Flush，没有已注册 Type 的 Runtime 也安装。RuntimeID 标识一次 Runtime 寿命；query 的 NATS Reply 必须精确等于同 namespace 下由 origin RuntimeID 推导的回复 subject。公开 DiscoveryCluster 查询由本地 Discovery Actor 在有限窗口内聚合这些快照。wire 及实现见[内部发现传输](contract.zh.md#内部发现传输)和 [discovery_network.go](../discovery_network.go)。

实现见 [network.go](../network.go)、[activation.go](../activation.go)、[factory_rpc.go](../factory_rpc.go) 和 [stream.go](../stream.go)。

### KV 与 stream

每个 bucket `B` 对应 stream `KV_<B>`，捕获 `$KV.<B>.>`。记录的完整 KV subject 为 `$KV.<B>.<key>`；`G` 表示激活 generation。

| 保存内容 | Bucket（B） | Key |
| --- | --- | --- |
| 不可变 Type 契约 | `actorxdist_<namespace>_types_v1` | `t.<T>` |
| 当前激活归属与状态 | `actorxdist_<namespace>_activations_v1` | `a.<T>.<I>` |
| 退出与释放证据 | `actorxdist_<namespace>_exits_v1` | `e.<G>` |

例如 `prod` 的契约记录位于 `$KV.actorxdist_prod_types_v1.t.<T>`，底层 stream 为 `KV_actorxdist_prod_types_v1`。条件写入及维护删除由 NATS SDK 执行；`dist-recover` 使用相同命名规则。记录状态、保留期和清理条件见 [Registry 协议](registry.zh.md)，实现见 [registry.go](../registry.go)、[maintenance.go](../maintenance.go) 和[恢复工具](../internal/recovery/recovery.go)。

### JetStream API

以下由当前使用的 nats.go v1.36.0 生成。`S` 是上述 stream 名，`K` 是完整 KV subject（`$KV.<B>.<key>`）。

| Subject | 用途 |
| --- | --- |
| `$JS.API.INFO` | 查询 account 存储配额与使用量 |
| `$JS.API.STREAM.INFO.<S>` | 初始化、KV 句柄获取、重连校验及容量检查 |
| `$JS.API.STREAM.CREATE.<S>` | 初始化时创建缺失的 KV stream |
| `$JS.API.DIRECT.GET.<S>.<K>` | 对开启 AllowDirect 的 bucket 读取指定 key 的最新值 |
| `$JS.API.STREAM.MSG.GET.<S>` | 维护扫描、读取原始记录；AllowDirect 关闭时也用于 KV 读取 |
| `$JS.API.STREAM.PURGE.<S>` | 维护时按 subject 和排他序号上限清理记录 |

Runtime 新建的 stream 开启 AllowDirect。Registry 使用点读和有界扫描，不建立 KV watch 或 JetStream consumer。SDK 使用独立的临时 `_INBOX.*` 接收 API 响应和 KV 写入确认。

`$JS.API.` 和操作路径属于 NATS 服务端 API，namespace 只影响其中引用的资源名。完整接口见 [NATS JetStream API 文档](https://docs.nats.io/reference/jetstream/api/)。

## 权限与部署要点

调用方发布激活、确认、所需停止控制及业务请求；提供方订阅自己提供的 Type、注册控制入口和实例业务入口。双方需要发布回复和 Stream 额度，并订阅自己的 inbox。confirm 与 stop 应按不同发布权限处理。

所有 Runtime 初始化均查询 stream，缺失时创建 bucket；纯调用方也会读取契约、归属与退出记录。契约发布、归属/退出条件写入和日常维护属于提供方路径。按上方名称模板，将权限限定到实际 namespace、Type/token、KV key 和 stream；各角色所需权限应分别配置。

每个 Runtime 还须有本 namespace 发现 query 及自身 reply subject 的订阅权限，以及 query 和其他节点 reply subject 的发布权限。这些 token 限定回复路由，不承担集群成员认证。

每个 namespace 使用三个 stream，并有独立 bucket 容量；account 总存储配额及用量仍共用，其他部署的用量也会影响 `$JS.API.INFO` 容量检查。Namespace 提供命名隔离，安全隔离需结合 NATS account、认证与权限，见 [NATS account 文档](https://docs.nats.io/learn/security/accounts-and-multitenancy)。服务端配置、容量调整及故障处理见[部署指南](deployment.zh.md)。
