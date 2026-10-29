# wire 契约与资源限制

[English](contract.md) | 中文

本文定义当前 v1 身份、schema、信封、错误和资源限制。使用层的完成与取消规则见[调用语义](runtime.zh.md)。数值是经本地序列化及预算算术校准的开发配置，不代表生产容量实测。

## 名称与规范身份

Type 为 1–64 ASCII 字节，匹配 `[a-z][a-z0-9]*([.-][a-z0-9]+)*`。ID 为 1–128 字节有效 UTF-8，不含 Unicode 控制字符。ID 内空格、点、斜杠、通配符均是数据，不直接进入 subject 段。不裁空格、不折叠大小写、不做 Unicode 规范化；视觉等价的 Unicode 字符串仍可表示不同 ID。省略 ID 及生成 generation、factory-token、关联/操作 ID 时，使用 crypto/rand 的 16 字节，以无填充 base64url 编为 22 字符；显式空 ID 无效。

Type/ID 段统一采用原始字节的 RFC 4648 raw base64url。严格解码拒绝填充、被忽略的换行、非零填充位，解码后重编码须一致。counter → Y291bnRlcg，orders-42 → b3JkZXJzLTQy。不透明随机 token 已编码，不二次编码。三个独立 KV 桶的 key 分别为 `t.<type-token>`、`a.<type-token>.<id-token>`、`e.<generation>`。名称错误返回 ErrSetup，token 不承担授权。

subject 使用 `actorxdist.default.factory.<type-token>`、`actorxdist.default.factory.<type-token>.<factory-token>.control.confirm`、`.control.stop` 和 `actorxdist.default.actor.<type-token>.<id-token>`。Factory queue group 是 `actorxdist.default.factory.<type-token>.workers`，不是发布目的地。每 Factory 一个普通 control.* 订阅，无 queue group，跨越其所有代及 FAILED，在任何 STARTING 归属发布前安装/Flush，在全部所属工作结清后移除。每个有效实例一个普通业务订阅。confirm/stop 分别授权，不回退共享队列。

以上为默认 namespace `default` 的名称；`WithNamespace` 同时配置消息路由、队列组和 KV bucket，不能只替换其中一项。完整名称及配置说明见 [NATS 技术参考](nats.zh.md)。

## schema 子集与指纹

| Go 形状 | v1 wire 规则 |
| --- | --- |
| bool、string 及具名形式 | 保留精确类型身份；字符串须有效 UTF-8 |
| int/uint | 限定可移植的 32 位有符号/无符号范围，身份仍区别 int32/uint32 |
| int8/16/32/64、uint8/16/32/64 | 精确范围；整数十进制，不接受小数或指数，不经 float64 中转 |
| float32/64 | 仅有限值，拒绝溢出、NaN/Inf |
| 指针、slice、array、map[string]T | 保留元素结构；指针/slice/map 可为 null；[]byte 用严格的带填充标准 base64，array 保留长度 |
| struct | 导出的非嵌入字段；JSON 名称精确匹配；无 omitempty 字段缺失则拒绝，可选字段省略后用零值 |
| JSON tag | 空/默认字段名、明确名称及可选 omitempty；- 排除字段；拒绝重名、嵌入、未知选项及 ,string |
| 递归具名类型 | 定义表加引用，重复 schema ID 不无限展开；拒绝值中的环 |
| actorxdist.Addr | 内置 addr-v1 节点；严格 version/type/id/可选非空 generation；任意嵌套层解码后关联接收 Runtime |
| nil | 仅可空节点允许；无结果描述不同于可空的具名结果 |
| interface、func、chan、complex、unsafe.Pointer、uintptr | 嵌套形式一并拒绝；仅核心无结果描述可 Call[any] 返回 nil |
| 自定义 JSON/Text 编解码、time.Time、匿名嵌入、实例化泛型具名类型 | v1 拒绝，不调用用户 codec，改用显式 DTO |
| actorx.Addr、两层 Context/Runtime、Reply/StreamReply 等执行能力 | 在普通 struct 处理前拒绝，包含指针及嵌套形式 |

别名沿用 Go 反射得到的原类型身份。具名 ID 是 `named(<JSON 引号包路径>,<JSON 引号名称>)`，内置类型使用 Go 名称。复合 ID 为 ptr(ID)、slice(ID)、array(N,ID)、map(string,ID)。匿名 struct ID 为 `struct(<规范字段 JSON>)`，每字段是 `[jsonName,typeID,omitemptyBool]`，按 JSON 名称的 UTF-8 字节排序。同 schema 不同具名类型仍包含不同包路径/名称。类型 ID 最长 1024 字节，超出拒绝。

schema 节点固定为 JSON 数组：`["scalar",goKind]`、`["ptr",elementID]`、`["slice",elementID]`、`["array",N,elementID]`、`["map",elementID]`、`["struct",fields]`、`["addr-v1"]`。定义为 `[typeID,node]`，具名定义在具名 ID 下记录其底层节点；节点引用 typeID，不使用 reflect 指针。[]byte 使用 `["bytes"]`。无结果使用 null，不产生定义。

指纹输入为紧凑 UTF-8 JSON 数组 `[1,handlers,definitions]`。handler 为 `[messageTypeID,resultTypeID-or-null,modes]`，modes 为 `["send","call"]` 或 `["stream"]`。handler 按 messageTypeID、定义按 typeID、字段按 JSON 名称逐字节排序；数组/标量顺序固定，使用 encoding/json 的字符串转义并启用 HTML 转义，无空白或末尾换行。对精确字节计算 SHA-256，输出 64 位小写 hex。生命周期绑定、函数、对象状态和构造函数地址不进入指纹。注册和真实绑定快照须完整相等，同 Type 仅一份不可变完整契约，新增 handler 也须新 Type。契约查询只读取已知 Type；Runtime.Discover 另行观察 ready 注册，不证明容量或未来可用性。

codec 拒绝重复/未知 JSON 字段、非法 UTF-8、尾随值、错误 null、类型/数值不符、值环、深度超过 32、值节点超过 8192、超过 256 个 schema 定义或 64 个普通 handler。契约最多 64 KiB。编码使用有限 writer 和有界遍历，不能先无限 Marshal 再检查。请求 payload/chunk 编码后最多 64 KiB，外帧最多 128 KiB。schema 遍历、codec 临时缓冲及嵌套 Addr 解码均须预留预算。

发送方在发布前核对 msg、模式和 R/C 与完整规范；接收方在 mailbox 前核对指纹、key/generation、消息/schema 和模式。内部 Call[any]/Stream[any] 桥接不能绕过检查。Addr 导出在 state 锁内取一致快照，默认 UnmarshalJSON 返回 ErrSetup，Runtime.DecodeAddr 严格解码且无 I/O。Send/Call/Stream 均已落实这些检查。

## 内部发现传输

发现使用独立的 v1 JSON 帧，不使用业务信封或单响应 Call 关联表。query 最多 1 KiB，必需 `version=1`、`query_id`、`origin_runtime_id` 和正整数 `deadline_unix_ms`（Unix 毫秒）；两个 ID 遵循 22 字符 opaque 身份规则。NATS Reply 必须精确等于 `actorxdist.<namespace>.discovery.v1.reply.<origin_runtime_id>`，广播 subject 为 `actorxdist.<namespace>.discovery.v1.query`。

reply 必需 `version=1`、`query_id`、`runtime_id`；可选 `entries` 包含 `{type,fingerprint,description?}`，可选 `truncated` 表示省略了后缀。Description 须为有效 UTF-8、最多 512 字节；Type、fingerprint 遵循上文规则，拒绝重复 Type。空目录以无 entries 的成功回复表达。可选 `error` 只允许 `full` 或 `unavailable`，此时不得出现 entries 或 truncated。严格解码拒绝重复/未知字段、null、错误类型、非法身份和超限帧。每条回复最多 256 项，**包含 QueryID 的完整编码帧最多 64 KiB**；编码超限时保留前缀并设置 truncated。

query callback 使用共享普通分派及 payload 槽；快照 Call 使用本地 Discovery Actor 的 Task 预算。reply callback 预留 payload/codec 槽并投递 typed 消息到该 Actor。本地回复丢失（包含 SDK slow-consumer 丢失）推进一个共享接收失败序号。连接故障使 transport epoch 失效并关闭已捕获的通知信号，重连校验后才允许新 epoch。传输层没有查询表或 collector。Close 撤销两个端点，汇合已进入 callback 及 dispatch，随后由 management 汇合快照 Task。公开 Cluster 查询由 Actor 持有 QueryID 关联并按完整节点快照原子合并。取消、Runtime 关闭、接收失败与 epoch 失效优先于成功完成；迟到回复不会重建查询。

## 统一信封

外层字段顺序为 version、kind、operation、type、id、generation、factory、correlation、deadline、message、mode、fingerprint、reply、control、payload、seq、items、bytes、status、error。仅省略不适用字段，version=1、kind、correlation 始终必需。未知版本/帧/字段或非法字段组合返回 ErrProtocol，严格 JSON 包含重复键拒绝。reply/control 最多 256 ASCII 字节，无通配符或空白；向自身接收回复时须属于接收 Runtime 配置的 inbox 前缀，不替代 key/generation/correlation 校验。回复回显 operation、key、generation、fingerprint、correlation、deadline，Factory 回复同时回显 factory-token；不能改变原请求身份。

| kind / operation | 额外必需字段与语义 |
| --- | --- |
| request / activate | type、id、correlation、deadline、fingerprint、reply、payload `{create:bool}`；首次选代无 generation/factory；create=false 不构造 |
| request / confirm 或 stop | type、id、generation、factory、deadline、reply；stop payload 含有界 cause 错误信封，subject Type/token 必须相符 |
| request / invoke | type、id、generation、deadline、message、mode、fingerprint、reply、payload；Call/Stream 另有 control 端点，Stream 含初始 items/bytes 窗口 |
| complete / 任意 | status=ok/error，仅 error 状态要求 error；Send 成功无结果，Call 成功为具体结果/无结果 null，Stream 必含最终 seq；Factory 成功 payload 含已确认 state/revision/generation/factory |
| chunk / invoke | 原请求身份，seq 从 1 连续开始，payload、bytes=编码 payload 长度，仅 Stream |
| cancel / invoke | 原请求身份及有界 cause，仅 Call/Stream，不表示新结果或受理 |
| window / invoke | 原请求身份，seq=最后连续消费序号，items/bytes=本次精确归还额度，仅 Stream；重放/膨胀额度为协议错误 |

错误完成在 payload 中携带受理证据：精确目标仅在 mailbox 前明确拒绝时可写 `{"admission":"not_admitted"}`，否则为 `{"admission":"unknown"}`。错误不推定回滚。Stream 最终 seq 为已产出 chunk 数，错误终止也携带；缺块、重复或乱序不能成功。窗口包含活动消费回调，仅在回调退出后归还；取消/失败后不启动新回调，所有返回路径先等待活动回调，再释放预算。终止/控制/取消/窗口帧不能创建关联。

Send 只在本地 Addr.Send 受理后完成；Call 需要有效最终结果、不增加 ACK；Stream 需要成功最终序号与完整消费。Stop 证明关闭受理（FAILED 另需释放），Wait 仅使用 Registry COMMITTED，Close 包含全部所属清理。同进程调用仍经过 NATS。

全部尝试均证明未发送时返回原校验/容量/context 错误。精确目标拒绝只证明该次未受理。可能已发送但缺所需 ACK/结果/终止时返回 ErrOutcomeUnknown 并保留传输/context 原因，不透明重放。旧代/非拥有者不能证明拒绝。不提供业务结果存储。激活不明使用 ErrActivationUnknown，按原 key/generation 核实，与业务结果不明分开。

## 有界错误

internal/errwire 实现 `{"version":1,"codes":[],"message":"...","sources":[],"truncated":true?}`。code 顺序固定为 closed、stopped、full、unhandled、reply_type、mode、setup、phase、chunk_size、type_unavailable、not_activated、contract、protocol、owner_unavailable、activation_unknown、outcome_unknown、exit_unknown、canceled、deadline，映射对应 actorx/actorxdist/context sentinel，唯一且完整保留，不随诊断截断丢失。未知 code 返回 ErrProtocol，任意业务 error 仅还原文字。

message 最多 512 UTF-8 字节；最多 4 个 source，每项 operation≤32、Type≤64、ID≤128、generation≤22 字节；编码信封最多 8192 字节。超出时保留最近来源及最深三个来源，设置 truncated，按 UTF-8 边界截断诊断，不截断分类；JSON 转义会超出总预算时进一步缩短 message。解码 error 支持 errors.Is，再包装、跨 Actor 返回后仍保留全部 code 和有界下游来源。收到 B 的确定回复不抹去 B→C 的 outcome_unknown/deadline；诊断区分本次结果及下游不确定性，不含业务 payload。真实独立进程测试覆盖 A→B 错误传播。

## 时间及开发资源配置

默认请求 5s，最长总请求 60s，控制/Flush 2s，KV 单次 1s，initCtx 无更早期限时初始化 10s。本地单调时间预算贯穿契约读取、选代、激活及通信，不逐阶段重置。wire deadline 为 UTC Unix 纳秒，并受本地开始+60s 限制。受支持主机时钟同步误差在 ±250ms 内；接收端取 min(wire deadline−当前墙钟,60s)，不为偏差延长期限。偏差可能提前拒绝，不授权归属或证明未执行，超出假设须另行验证。

管理方持有创建寿命，构造/启动 30s 后请求取消；New/Started 不返回仍占原 token。KV 每批最多尝试 3 次，受调用/阶段总期限限制，20/40/80ms 有界退避。同代 owner 确认遇到 CAS 冲突时，在调用方期限及内部 2s 上限内重试；其他失败在总尝试次数达到第三次时停止。取得归属后的清理/提交每代保留一个 token，按 1、2、4…30s 间隔重试，每批最多 3 次 KV 操作，直到证据完整或显式恢复；最后等待者离开不终止管理，不每次重试新增 goroutine。Go 取消不能强制终止应用回调/构造函数。

下表列出默认开发配置。公开执行/网络参数及含义见[容量配置](runtime.zh.md#容量配置)，其他协议、存储及管理限制继续由内部管理。

自定义配置中，记 I=Instances、R=Requests、S=Streams。执行实例与所属激活各自最多 I 个，每 Actor 的 mailbox/pending/tasks 使用配置值。业务核心调用关联、出站请求、入站关联、共享回复关联及解析工作分别最多 R 个；Send ACK 等待最多 ceil(R/2)，解析/Wait 等待者总计最多 max(256,2R)，每地址 state 仍最多16个。入站/出站流共享 S 个额度，窗口合计预留 S×4 项 / S×64 KiB。

内部业务分派队列和保留 payload 各使用 max(32,ceil(R/4)) 个槽，控制队列使用 max(64,ceil(R/2)) 个槽。每个业务订阅和两个延迟 Call/Stream inbox 订阅各允许 max(8,ceil(R/16),ceil(S/2)) 个待处理帧，每帧最多128 KiB。记 T 为本 Runtime 已注册 Type 数，C=Creating、K=CreatingPerType。两个常驻发现订阅使用相同 pending 帧数，query/reply 每帧分别限 1/64 KiB。应用订阅合计最多 I+2T+4 个。Factory 任务容量 W=max(8,min(C,K,I)+4)，mailbox=max(64,2W)、pending=max(128,2W)、调用关联=max(64,C)，额外任务槽保留控制/清理余量。Types 可选限制 T，默认不限制数量。Registry 容量及 worker 数不自动增加；负数及槽/字节预算算术溢出在 I/O 前拒绝。

记 H=ContractCacheTypes（默认64）。调用方缓存最多保留 H 个 Type 索引和 H 份不同契约，单份≤64 KiB；指纹相同还须完整字节比较一致才能共享规范字节。因此驻留规范字节≤H×64 KiB（默认4 MiB，H=1024 时为64 MiB）。该上限不含 Type 字符串、map/list/对象开销、SDK 读取缓冲和在途快照，不是 Runtime 总内存限制或独立字节配额。条目按需增长，不按 H 预分配全部契约。miss 沿用请求/payload/codec 及 Registry I/O 预算，Registry 读取在缓存锁外执行；读取失败不缓存，冲突回填返回 ErrContract。

| 资源及持有者 | 局部/Runtime 上界 | 取得→释放；满载 |
| --- | --- | --- |
| 类型声明/Factory | Types 可选限制本地声明数（默认不限），64 handler/Type，256 schema 定义/契约，64 KiB/契约 | 本地注册槽→Unregister/Close 结清，已发布契约保留；无效/重复注册 ErrSetup，Type 配额 ErrFull |
| 调用方契约缓存/network | ContractCacheTypes 个 Type 索引（默认64），共享契约数≤Type 数，驻留规范字节≤H×64 KiB（默认4 MiB），不含开销和在途快照 | miss→LRU 淘汰或工作结清后的 Close；命中刷新顺序，满时淘汰一个 Type，最后缓存引用移除时回收共享字节；项数已满本身不返回 ErrFull |
| 业务引擎/核心 | 64 个业务实例；另独立计数最多64个所属激活，含构造、运行、FAILED、待提交；每实例 mailbox16、pending32、worker4；calls128；本地流 8×64 KiB 外加活动 chunk | 按实际归属预留，执行/清理结清才释放；复用核心 ErrFull |
| 管理引擎/核心 | T 个 Factory 加一个本地 Discovery Actor，mailbox/pending/tasks 按上文推导（默认每个 64/128/8）；calls=max(64,C)；流 1×4 KiB 外加活动 chunk | 与业务引擎独立；Discovery 独立预留实例，不占用户 Types；保持到提交结清，工作开始前 ErrFull |
| 本地发现快照/Discovery Actor | 每个任务最多保留 256 项、64 KiB 编码快照数据；共享该管理 Actor 的任务及完成预算 | 扫描 ready 注册→串行完成或丢弃；条目/字节超限保留排序前缀，Truncated=true、EndReason=limit；取消或失败返回零值报告 |
| Runtime 激活账本 | Creating 个并发创建（默认16），每 Type 最多 CreatingPerType 个（默认4）；属于 64 代预算子集 | CAS/New 前预留→提交/接管或完整回滚；FAILED 保留代槽，claim 前 ErrFull |
| 解析等待者/Addr 与 Runtime | 每 state16，总计256 | 加入解析前→自己的取消/结果；工作不随等待者结束；ErrFull |
| 本地子实例/业务引擎 | 与逻辑实例共享 64 个在用实例槽，与持久代槽分开 | 普通本地 Spawn 的 Bind 前预留→核心完整退出；使用已有 Channel 接缝，不能成为无界旁路 |
| 网络关联/Runtime | 出站≤128，Send ACK 子集≤64；入站业务关联≤128；共享回复关联≤128 | 发布/分派前→网络结清且回调退出；有效识别时 ErrFull，不从 SDK 丢帧推定未执行 |
| 应用订阅/Runtime | ≤I+2T+4：共享T、专属T、业务I、延迟 inbox2、发现2；业务各 pending8 帧/1 MiB，Factory 订阅各8 帧/1 MiB；默认执行/网络配置下累计544+16T 帧 / ((66+2T) MiB + 520 KiB) | subscribe 前→退订并等待已进入回调；SDK 丢失为异步故障/可能结果未知 |
| SDK Registry 请求/Runtime | 总并发8（普通≤6，控制/提交预留2）；强制 UseOldRequestStyle，临时订阅≤8，每个 channel8；≤64 帧/8 MiB | 显式 context 的 SDK 调用前→返回/退订；不进入应用关联表 |
| 网络分派 | 业务队列32 帧/4 MiB、8 worker；控制/取消队列64 帧/8 MiB、4 worker | 复制帧入队前计费→分派返回；有效识别的满载 ErrFull，不按消息创建 goroutine |
| codec/Runtime | 编解码并发槽8，各≤128 KiB 输入+128 KiB 输出，共2 MiB；深度32/节点8192 | 遍历/复制前→交给其他已计费持有者或丢弃；ErrFull/ErrChunkSize |
| 网络保留 payload/Runtime | 32 缓冲/4 MiB，关联另留≤2 KiB 元数据/项 | 持有前→最终持有者释放，无未计费副本 |
| Stream 消费窗口/Runtime | 入站/出站合计16 流，各4 项/64 KiB；总预留64 项/1 MiB，包含活动回调 | 开始前预留整个窗口→回调退出且关联关闭；ErrFull，帧队列另计 |
| Registry 扫描/工作 Runtime | 1 scanner，每页64 个序号位置、每秒≤1 页、最多32 个候选，不建立 KV watch | 扫描前→本页/尝试返回；满载暂停，调用方不执行写维护 |
| 提交/控制任务/Runtime | 使用已有64 代条目，每代最多1 工作，保留管理/控制队列及2 个 SDK 槽 | 清理开始→核实提交/释放，不因满载丢弃证据 |
| 诊断/Runtime | 非阻塞64 事件，各≤1 KiB，溢出合并计数 | 回调→排空/关闭，不在 SDK 线程执行任意用户回调 |

默认执行/网络配置、注册 T 个 Type 时，两个核心引擎合计最多 1088+64T mailbox 槽、2176+128T pending、264+8T worker、192 调用关联，再受较小网络/受理限制约束。应用代码保留的业务对象及 payload 引用不构成整个进程内存保证。SDK socket/parser、每个分派器在途帧及传输写缓冲另计：server max_payload=128 KiB，SDK 普通写缓冲32 KiB、重连缓冲禁用（该 SDK 用 -1；0 会恢复默认值）；decoder 无法撤销 SDK 已完成的分配。该配置以可信、按约配置的服务为前提。SDK 故障回调有界（含最多 Streams 条入站流的取消）、非阻塞，转交有界诊断；替换生命周期/错误控制的原始用户回调在 Option 规范化时拒绝。

淘汰只释放缓存引用，在途调用继续持有不可变快照，无须固定 LRU 项；淘汰后重读可能与旧快照短暂并存。淘汰不删除 Registry 契约、不撤销注册、不停止 Actor、不改变代次。Close 等待已受理工作结清后清空缓存，并拒绝迟到回填。

TestSizingBaseline 实测代表性最大帧/记录/错误样本并核对预算算术。默认64代×每代8次33 KiB 状态/终态/标记写入余量=16.5 MiB，低于每个128 MiB 桶30% 余量，仅是尺寸估算，不是全局空间预留。时间、CPU、真实记录开销、吞吐及多提供方进程存储竞争须按部署校准，不把这些数值宣传为通用推荐。

## 回复与流控资源

解析工作与等待者分开计费：Runtime 最多 Requests 份在途解析（默认128），全部等待者取消后仍保留至工作完成。每代生命周期监视及本地 Wait 等待工作各最多一份，总计各 Instances 份（默认64），随代账本结清退出。

激活与 Send/Call 共用一个延迟建立的 Runtime inbox，最多 Requests 个关联（默认128），单结果关联只容纳一帧；Call 取消向原业务 subject 发布，经控制队列匹配原回复能力与身份。第二个 Runtime inbox 按序接收 Stream 消费窗口（`_INBOX.<runtime>.control.<correlation>`），chunk 携带该控制能力，chunk/终止仍经第一个 inbox 回复。窗口不经过共享分派 worker，不按流新增订阅。

Stream 使用预留的4条/64 KiB chunk 队列及单个终态/错误槽；最多 Streams 个网络监视工作（默认16），释放预算前结清。各缓冲及关联仍受上表累计限制。


## 解码分配限制

静态反射类型大小最多128 KiB；解码在 reflect.New/MakeSlice/MakeMap 前保守累计128 KiB值存储，超限 ErrChunkSize。编码缓冲及保留回复共用32槽，codec并发8；严格 JSON 检查及反射辅助结构另外受输入字节、类型/节点上界约束，不把编码缓冲算术解释为整个 Go 堆上界。
