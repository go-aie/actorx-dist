# Registry 协议

[English](registry.md) | 中文

本文说明当前归属、终态与维护协议，基于 nats.go v1.36.0、nats-server v2.10.18 的 JetStream KV。版本与资源配置用于可复现开发。

## 配置及请求归属

NATS Core 传递不持久化的业务消息，文件型 JetStream KV 保存元数据。不使用 mirror、source、republish、压缩或外部 domain。开发 server max_payload=131072 字节、文件配额512 MiB、内存64 MiB；独立 account 存储配额384 MiB，至少允许3 stream、8 consumer，不让其他用户消耗该 account 余量。本地副本数1，复制部署须单独验证。

| KV 桶/底层 stream | key | MaxValueSize / MaxBytes | History / TTL / replicas |
| --- | --- | --- | --- |
| AD_TYPES_V1 / KV_AD_TYPES_V1 | t.<type-token> | 65536 /16 MiB | 1 /0 /1 |
| AD_ACTIVATIONS_V1 / KV_AD_ACTIVATIONS_V1 | a.<type-token>.<id-token> | 16384 /128 MiB | 1 /0 /1 |
| AD_EXITS_V1 / KV_AD_EXITS_V1 | e.<generation> | 16384 /128 MiB | 1 /0 /1 |

启动核对 KeyValueConfig 及 StreamInfo：文件存储、DiscardNew、MaxMsgsPerSubject=1、MaxAge=0、AllowRollup=true、DenyDelete=true、DenyPurge=false、预期 subject、尺寸/副本数，无 transform/mirror。建桶幂等，不能仅凭桶同名接受配置，不静默修改不兼容部署。类型规范永不过期。终态从 COMMITTED/RELEASED 版本的服务端创建时间起保留24小时，FAILED_HELD/PREPARED 不按年龄淘汰。沿用 wire 的 ±250ms 时钟假设，维护额外等待500ms再判过期，不设置统一 TTL。

连接选项在初始化按快照应用一次；框架在用户认证/TLS 选项之后固定禁用重连缓冲（SDK 值 -1；0 会恢复默认8MiB）、请求方式、订阅/就绪/故障处理和关闭规则，不借用连接。断线后暂停新增，保留 generation/factory-token，重连核实订阅/归属后恢复；已退出订阅不恢复。初始化失败回滚本地资源，已发布且匹配的全局类型规范保留。

每次 Registry 操作先取得8个 SDK 槽之一（普通6、控制/提交预留2）。为该次操作构造 `nc.JetStream(nats.Context(opCtx))`，从中取得 js.KeyValue(bucket)：旧 KV Create/Update/Delete 方法不接收单次 context，不能复用带前次 context 的 handle。context 覆盖查桶与操作，最多1s且受阶段剩余期限限制。强制 nats.UseOldRequestStyle，使 SDK 请求使用有界临时 inbox，不建立隐藏异步回复 mux，返回时 SDK 退订。不使用 KV Watch/Keys/History/PurgeDeletes 的全量物化，采用下述有界序号扫描。

每逻辑批次最多3次 SDK 操作；Create 在删除标记场景内部最多3次 RPC，所以底层最多9次顺序请求，仍受同一 deadline/槽限制。批次超时只归还 I/O 槽，已取得的代、清理证据和待提交工作仍归 Runtime 跨批次持有。重连不能授权取得旧 key。

## 记录与条件操作

记录统一含 version=1 及稳定22字符 operation ID。当前记录含 key、generation、factory-token、契约指纹及状态。终态含原 key/generation/factory-token、原 cause 错误信封、created_instance、evidence（no_instance/spawn_rollback/local_wait）、cleanup digest、预期处置、prepared revision、已知的 release operation/revision、提交标记/处置。digest 为不含可变提交字段的规范清理证据 SHA-256，不保存业务对象或任意 Go error。

写前由资源账本保留 operation ID 及完整预期记录。revision 是返回的 uint64 stream sequence，不是 generation。不能刷新 expected revision 后不重新读状态及全部条件就重试过时转换。变更同时要求 key/generation/factory、预期状态及 revision，仅 key 相同不足以授权。

| 转换 | SDK 调用及条件 | 成功/丢确认后的核实 |
| --- | --- | --- |
| 发布类型 | types.Create(t,canonicalContract) | 返回 revision；已存在时规范字节/指纹须完全相同，否则 ErrContract，不执行 New |
| 取得 STARTING | 不存在/已删时 current.Create(a,record)，仅观察到 INACTIVE 才 current.Update(a,record,r) | 发送前保留候选 generation/factory/op；出错后读精确 key，相同 op/gen/factory 证明取得，冲突活动代则失败，不明保持 ErrActivationUnknown |
| 确认候选 | generation/factory/state 有效时 current.Update(a,sameStateWithConfirmOp,r) | 返回 revision 证明该瞬间归属；丢回复可读相同 op 或重新做有条件确认，最多3次，INACTIVE/RELEASED 不得绑定 |
| 发布 READY | Spawn 成功、订阅 Flush、复查停止边界后 current.Update(a,ready,r) | 返回 revision 或读精确 op/state，核实前关闭业务门闩；发布不明不能直接判 FAILED |
| 关闭受理/STOPPING | 先本地门闩，再对同代/factory current.Update(a,stopping,r) | 普通 Stop 成功证明本地受理已关闭，不证明 Wait/终态；保留本地关闭事实并补写元数据，不再开始构造 |
| 准备终态 | 完整清理证据齐备后 exits.Create(e,prepared) | 返回终态 revision p；已有则比对原 key/gen/factory/cause/evidence/digest/预期处置，不一致为协议错误，不覆盖 |
| 发布 FAILED | 已清理、有 PREPARED、仍属本 Factory 的 STARTING 时 current.Update(a,failed,r) | 返回 revision 或精确 op；已受理停止走 STOPPING/释放，不回退后续状态 |
| 释放当前归属 | 有 PREPARED/FAILED_HELD 及清理证据，仍属原 STOPPING/FAILED 时 current.Update(a,inactive,r) | INACTIVE 保存 release op、终态 key/digest、prepared revision；账本保留返回 q；丢确认时相同 INACTIVE op/digest 可核实，否则按崩溃表 |
| 提交终态 | exits.Update(e,committed,p) | 仅在核实 FAILED 保留/归属释放后；RELEASED 保存 digest 及 q/release op，返回终态 revision c；丢确认读原代匹配证据/处置，不仅检查当前状态 |
| 释放 FAILED_HELD | current.Update(a,inactive,r) 后 exits.Update(e,released,p) | 原 cause/digest 不变，需同样的 release op/q，RELEASED 确认前 Stop 不成功，保留期从此起算 |

Get/GetLastMsg/watch/缓存只提供候选，初次绑定需成功条件确认/claim。已有候选按原 factory-token 定向，不交给随机 Factory。queue 分发不证明唯一归属。最后等待者取消后 Factory 仍持 STARTING 工作。New nil/panic、绑定不符及失败 Spawn 先完成回滚再 FAILED；已受理 Stop/Close 则释放，不新增保留失败。

READY 证明同步 Started 及订阅就绪，不证明异步业务就绪。普通 handler error 只结束请求，panic/自停/Stop/Close 结束实例。失败激活 Wait 可返回非空 Exit.Reason 和 nil 观察错误但仍持归属，FAILED_HELD 必须 Stop 或所属 Runtime.Close 结束。纯调用方关闭、协作对象结束均不停止远端。独占临时对象保留原 Addr，用独立有界收尾 context 执行 Stop→Wait。

## 崩溃与不明结果

| 崩溃/丢回复时的持久事实 | 可得结论 |
| --- | --- |
| claim 未确认，可能已写 STARTING | 按原候选 generation/op 核实，不因调用方超时生成替代代次 |
| STARTING 存在，构造结果仅在已死进程 | 保留归属，没有完整清理证据，Wait 不成功；隔离原进程后显式恢复 |
| 仅 PREPARED，当前仍 STOPPING | 有清理证据、未提交释放；Wait 不成功，存活原拥有者可条件释放/提交 |
| PREPARED 与匹配 release op/digest 的 INACTIVE | 原拥有者可核实补交；操作员隔离后可用精确持久证据恢复，重启 Runtime 不继承 token |
| PREPARED；新代覆盖 INACTIVE；旧终态未 COMMITTED | History=1 可能已删除 q。新代存在不能证明原释放操作及其清理证据关联。只有仍存活且保留成功 q/释放证据的原拥有者可提交；若确认丢失且无法恢复证据，持续 ErrExitUnknown，显式恢复也不能伪造 RELEASED Exit |
| COMMITTED/FAILED_HELD | 旧 Wait 可观察失败，key 仍保留到所属 Stop/Close 释放 |
| COMMITTED/RELEASED 及原证据 | 不依赖新代/当前记录删除，旧 Wait 成功；重复 Stop 不影响新代 |
| 终态缺失/过期/矛盾 | ErrExitUnknown，原管理者不可达可另有 ErrOwnerUnavailable；不从不存在、INACTIVE、新 READY 或操作员声明合成 Exit |

不承诺多 key 事务，也不承诺每个崩溃点都能成功 Wait。旧终态提交前新代可以取得 INACTIVE：该间隙属于保守故障契约，存活拥有者证据可补交，进程死亡并缺持久释放证据时不可。Runtime 保留不明提交并拒绝成功 Close；安全恢复可释放/隔离归属，但不能把旧不明结果变成成功。不以无限立即重试替代缺失证据。

## 条件清理与物理回收

对 INACTIVE 候选读取 key/gen/revision r，先核实精确 COMMITTED/RELEASED 终态，再 `current.Delete(a,nats.LastRevision(r))`。成功写入删除标记，不是物理回收。冲突放弃候选；丢确认后有界 Get 最新值，仍为同候选仅用原 r 重试，不存在/新代则不再修改。原始删除标记通过 `js.GetLastMsg("KV_AD_ACTIVATIONS_V1","$KV.AD_ACTIVATIONS_V1."+a)` 核对 Sequence、Subject、KV-Operation；KV.Get/GetRevision 会隐藏删除标记。

序号 scanner 先 js.StreamInfo(stream) 得 FirstSeq/LastSeq，每秒最多64个 js.GetMsg(stream,seq) 位置，设 deadline、跳过空洞，仅保留32候选，游标回绕继续，不累积全部 key、不建 watch consumer。将原始消息当候选前核对 subject、operation、记录尺寸。每工作 Runtime 一个 scanner，纯调用方没有；变更前重新读取，重启可安全重扫。

物理回收先检查资格，再读当前原始删除标记序号 d，调用 `js.PurgeStream(stream,&nats.StreamPurgeRequest{Subject:exactKVSubject,Sequence:d+1})`。Sequence 是排他上界，仅清除≤d；并发新记录序号更大，不会被删。不允许无序号上界的 Keep=0、无条件 key Purge、DeleteMsg（KV 禁止），也不使用 SDK PurgeDeletes：该方法扫描标记后按 subject 清理，没有序号保护。丢回复可用同一 d+1 重试，不能改成无限清理。逻辑删除、服务端消息数/字节回收、实际磁盘复用须分别观察。

终态回收要求 COMMITTED/RELEASED 且年龄≥24h+500ms。先确认当前记录不存在、已是新代，或按前述规则安全条件删除；PREPARED/FAILED_HELD 不授权清理。随后对观察到的终态 revision c 执行 `exits.Delete(e,nats.LastRevision(c))`，再按标记序号回收。revision 变化则放弃。History=1 限制历史值，但标记在回收前仍占空间。类型规范不可变、持久保留，不自动删除。

## 容量及恢复边界

新 claim 前读取 StreamInfo/AccountInfo，观测新鲜度≤1s。当前/终态桶或 account 字节使用≥70% 时拒绝新增激活；新类型在类型桶≥90% 时拒绝；用量不可读也不准入。确认 claim 前的容量拒绝返回 ErrFull 并保留有界存储原因；claim 可能已成功则同时保留 ErrActivationUnknown。默认每 Runtime 的64代及每代8次(16KiB+16KiB+1KiB)写入余量约16.5MiB，但各桶/account 可能先满。真实服务须测 payload、NATS header、subject、历史、标记、副本及文件开销；本地 token 不能原子预留全局空间。

新增被拒时保留控制/提交槽。桶/account 硬配额、磁盘故障或并发 Runtime 仍可能阻止最终写入。关闭受理、尽量完成实际清理，保留当前归属/PREPARED/FAILED_HELD 及原拥有者证据、继续跟踪提交；所需 COMMITTED 不齐时 Wait/Close 不成功。已提交 FAILED_HELD 可观察但未释放。不删未完成证据腾空间。

运维顺序：暂停新增激活→检查用量及待提交代→扩容/修复或仅条件回收合格历史→原拥有者核实补交→确认 Wait/Close 和容量后恢复。原拥有者已死时先隔离进程/订阅，再按精确 key/generation/revision 恢复，拒绝过期请求。不合成缺失 Exit 或业务结果。

来源：[固定 KV SDK](https://github.com/nats-io/nats.go/blob/v1.36.0/kv.go)、[固定 stream SDK](https://github.com/nats-io/nats.go/blob/v1.36.0/jsm.go)、[固定 server 配置](https://github.com/nats-io/nats-server/blob/v2.10.18/server/stream.go)。以上协议、资源选择及保守不明结果规则为 dist 的设计决策。
