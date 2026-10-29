# 连接与部署环境

[English](deployment.md) | 中文

本文说明服务配置、权限、升级和故障处理。默认配置用于单服务开发；调用行为见[调用语义](runtime.zh.md)，内部存储规则见 [Registry 协议](registry.zh.md)。

## 服务配置

本机快速运行使用模块中的 [examples/nats.conf](../examples/nats.conf)。以下是带用户名认证的独立配置示例，保存为 nats.conf；不要与快速运行的免认证 URL 混用。当前开发版本为 Go 1.27、nats.go v1.36.0、nats-server v2.10.18。存储目录须可写，共享部署还需配置 TLS 和权限。

```conf
host: "127.0.0.1"
port: 4222
max_payload: 131072
jetstream {
  store_dir: "./dist-data"
  max_file_store: 512MB
  max_memory_store: 64MB
}
accounts {
  APP {
    jetstream {
      max_file: 384MB
      max_memory: 32MB
      max_streams: 3
      max_consumers: 8
    }
    users: [{user: "dev", password: "change-me"}]
  }
}
```

启动服务：`nats-server -c nats.conf`。Runtime 默认连接 `nats://127.0.0.1:4222`；覆盖 URL 或认证：

```go
rt, err := actorxdist.NewRuntime(ctx,
    actorxdist.WithNATS("nats://127.0.0.1:4222", nats.UserInfo("dev", "change-me")),
    // 创建成功后通过 rt.Register(ctx, spec) 注册工作类型。
)
```

WithNATS 的空 URL 也采用默认地址；重复配置以最后一次的完整 URL/选项快照为准。拒绝非法 URL、nil SDK Option、用户替换生命周期/错误回调、自定义 dialer、NoEcho 和初连隐式重试。认证与 TLS 的 SDK 选项保留。Runtime 独占连接；初始化 context 默认最多10秒，仅约束初始化，成功后由 Close 管理寿命。

Register 核对该 Type 的契约并等待入口就绪；失败时自动清理本次本地资源，已经发布的全局不可变契约保留。失败时可能同时返回句柄，可调用 Unregister 等待清理。纯调用方没有 Factory 订阅或写维护。初始化时序见[架构](architecture.zh.md#激活与业务路径)。

## 存储与权限

同一 NATS account 内，相同 namespace 的 Runtime 共享消息路由和 KV bucket。独立部署使用不同 `WithNamespace` 值，默认值为 `default`；subject 清单及 JetStream API 权限范围见 [NATS 技术参考](nats.zh.md)。每个 namespace 使用三个 stream，多套部署需要相应提高账户 stream 数量与存储配额；上方示例仅容纳一套。

通过 [WithExecutionLimits 和 WithTransportLimits](runtime.zh.md#容量配置) 配置单进程执行和网络容量。提高本地容量不会扩容以下 KV 桶或账户配额。

Runtime 幂等创建三个 KV 桶，已有配置不兼容则报错，不自动修正。类型桶为16MiB/值64KiB；当前归属与终态桶各128MiB/值16KiB。全部文件存储、History=1、无 TTL、DiscardNew、副本数1。完整配置和保守证据规则见 [Registry 协议](registry.zh.md)。改变完整 wire 契约须使用新 Type；类型规范存在不表示 Factory 在线。

权限应分别授予：JetStream 账户/stream 信息、所需 KV 的读取与条件写入、受序号限制的维护 purge、Factory 共享入口、专属控制入口和临时 inbox。`actorxdist.default.factory.<type-token>.<factory-token>.control.confirm` 与 `.control.stop` 是不同发布权限；接收方使用普通 `control.*` 订阅，不使用 queue group。严格部署需要结合实际 Type/token 范围授权。Send/Call/Stream 调用方需要发布到 `actorxdist.default.actor.<type-token>.<id-token>`，提供方进程需要订阅其声明 Type 下的业务入口，双方需要发布回复及订阅自己的 `_INBOX.>`。Call/Stream 取消使用原业务 subject，不增加订阅或额外控制权限。

连接和控制容量的精确上限见 [wire 资源表](contract.zh.md#时间及开发资源配置)。

断线关闭新增网络受理，重连保留原 factory-token，复核桶、类型、已持有代的归属及订阅 Flush 后恢复；重启生成新 token，不继承旧归属。没有自动接管或业务重放。`rt.Diagnostics()` 非阻塞排出最多64个有界事件及累计溢出数；异步 SDK 故障、分派溢出和维护失败可观察，不在 SDK 回调中执行用户逻辑。

提供方 Runtime 有界维护已释放元数据；终态保留24小时并加500ms保护，PREPARED/FAILED_HELD 不按年龄清理。条件删除和物理回收规则见 [Registry](registry.zh.md#条件清理与物理回收)。

Stream 消费确认发布到 chunk 中携带的拥有者 inbox；双方均须具备相应 `_INBOX.>` 发布权限。每个 Runtime 的两个 inbox 由全部调用与流共用，不按流新增订阅。

## 显式恢复与存储修复

正常停机先等待所属 Runtime.Close。崩溃或强制退出后，先在进程管理器/容器平台确认旧进程、后台工作及业务订阅不会重新生效；断网、暂停、TTL 或一次探测失败均不足以证明隔离。恢复命令不负责自动终止进程，也不接管业务状态。

以下以 [Counter 示例的本机开发服务](../examples/counter/README.zh.md#启动-nats)为例，从 actorx-dist 仓库根目录构建并只读检查。工具不创建 Runtime、Factory 或业务订阅；使用上面的认证配置时，将 URL 改为 `nats://dev:change-me@127.0.0.1:4222` 或提供凭据文件。

```sh
go build -o /tmp/dist-recover ./cmd/dist-recover
/tmp/dist-recover -nats nats://127.0.0.1:4222 -type counter -id orders-42
```

检查输出中的 namespace、key、generation、factory、revision、current 及 terminal。`-namespace` 默认为 `default`，自定义部署必须在检查和 apply 时都传入对应名称；快照和审计记录包含 namespace。确认旧进程隔离后，以本次检查的精确值执行；`-creds` 支持 NATS 凭据文件，TLS 可使用 tls:// URL 和系统信任配置：

```sh
/tmp/dist-recover -nats nats://127.0.0.1:4222 \
  -type counter -id orders-42 \
  -apply -generation '<generation>' -revision 123 \
  -confirm-stopped -reason 'incident-42: old container removed; work and subscriptions fenced' \
  -audit /durable/operator/dist-recovery.jsonl
```

默认不写入。apply 必须有隔离声明、1–960字节原因、精确 generation/revision 和可写持久审计路径；审计意图先写入并 fsync，再执行 CAS，每个结果也 fsync。审计文件按运维要求留存。工具使用已有桶，不修改配置，不自动扫描/清空历史。过期 key/generation/revision 或矛盾证据均拒绝；操作失败或结果不明时重新只读核实，不能重复使用旧 revision 或改为无条件删除。

存在匹配 PREPARED 或 FAILED_HELD 清理证据时，条件释放当前记录，再条件提交原代 RELEASED；匹配 INACTIVE 的持久释放凭据可补交。已提交 RELEASED 只核对、不影响新代。缺少终态证据时，只条件删除精确归属，审计记录 `released-without-exit`；不生成 Exit。旧 PREPARED 的释放凭据已被新代覆盖且原进程已死时，不从新代推导旧代终态，保留 ErrExitUnknown。恢复后用新 Addr 激活，旧 Addr 始终固定旧代。

容量故障先暂停新增激活，检查桶/account 用量与待提交代。70%软水位及独立控制槽为已有代推进留余量，但不能保证硬配额、磁盘故障或多 Runtime 并发下总能提交。扩容桶/account 或修复存储；只能条件回收 COMMITTED/RELEASED 且满足保留期的历史，不删除 PREPARED、FAILED_HELD 或失联归属腾空间。原拥有者仍存活时保留其连接和管理工作，修复后核实补交，重复 Wait/Close 确认完成；不要对仍运行的拥有者执行显式恢复。原进程已死且缺证据才走上述隔离和精确恢复流程。

## 契约升级

完整契约包含全部普通消息、带包路径的 Go 类型身份、schema、结果及模式。注册在 nil receiver 上读取 Bindings，不执行 New 或 Started。同 Type 的每个提供方进程必须完整严格匹配；即使既有消息不变，新增 handler 也不兼容。提供方进程和实例退出后，Registry 仍保留类型契约。

仅实现变化、完整契约不变的滚动更新：先启动新提供方进程，等待就绪日志，再优雅 Close 旧提供方进程。既有代保留原拥有者直到完整退出，新进程不继承；新 Addr 才可选择下一代，不包含状态迁移。

契约变化时声明新的稳定 Type，例如 example.counter.v2。身份与 schema 不变的 DTO 保留原共享包，变化的 DTO 使用独立包或新的具名类型。并行部署新旧 Type，将升级调用方切到新 Type；旧调用方排空后显式结束旧对象并 Close 旧提供方进程。同 ID 在不同 Type 下状态独立；旧终态保留到约定期限。不要改写已存 Type 记录或以清空存储作为升级策略。

## 多提供方进程与多机器

本机多进程运行方式见 [Counter](../examples/counter/README.zh.md)、[Click](../examples/click/README.zh.md) 与 [Chat](../examples/chat/README.zh.md)，所有角色连接同一服务/account。

实际跨主机时，在可达网络部署相同固定版本的 NATS，将客户端监听从 127.0.0.1 改为所需网卡，配置防火墙、认证、TLS 及上述权限。服务使用持久可写存储，主机时钟保持 ±250 ms 同步。各主机构建同一 provider/consumer 程序，统一通过 -nats 传入可达 URL。在提供方主机启动 provider，等待就绪后从调用主机运行相应 consumer 程序，按示例核对输出和同 key 累计结果。保留本地独立消费工具时，也可启动其 fixture provider，再从调用主机以 `consumer.py source --consumers-only --nats <服务URL>` 验证完整消费用例。

记录服务配置、主机身份、版本、依赖 manifest、就绪日志及输出。源码消费不因主机数量改变而成为发布证据。单服务 Replicas=1 不提供复制存储或自动容灾保证；实际跨主机网络及复制配置尚未验证。
