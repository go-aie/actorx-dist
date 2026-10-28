# 核心概念

[English](concepts.md) | 中文

理解 actorx-dist 先区分三件事：**逻辑对象是谁、当前运行哪一次激活、手里的地址指向哪一代**。业务仍使用 actorx 的普通 Actor、Bindings 和串行 handler；actorx-dist 增加寻址、按需激活与跨进程通信。

```text
ActorKey（Type + ID，跨代不变）
    └─ generation（一次激活，退出后不复用）
           └─ worker 中的 Actor 实例（内存状态与资源）
Addr 首次选代后固定；新 Addr 才能选择下一代。
```

generation 是不透明标识，不要求递增，也不同于 KV 条件更新的 revision。

例如 `counter / orders-42` 首次调用激活一代并累计到10。该代完整退出后，同 key 的新地址可激活另一代，计数从0开始；旧地址继续指向旧代。逻辑身份延续不意味着内存状态延续，状态恢复由业务自己的存储负责。

## 声明与启动

工作进程用 `rt.Register(ctx, spec)` 声明创建能力，`ActorSpec[T]` 提供 Type 和 New，T 是实现 actorx.Actor 的具体 struct 指针；纯调用方省略注册。Runtime 拥有连接、本地实例与管理工作。NewRuntime 成功表示执行、连接和存储就绪；Register 成功表示该次注册的契约和激活入口就绪，此时尚未构造业务对象。启动和运行期间均使用同一注册接口。通常忽略注册句柄，退出时由 Runtime.Close 统一撤销并等待收尾；需要独立控制某个 Type 的寿命时，再看[注册与撤销](runtime.zh.md#注册与撤销)。

取得创建权后才调用 `New(ActorKey)`，每次返回新的非 nil 对象。New 只做短小本地构造，不启动 goroutine 或获取需清理的资源；依赖可由闭包捕获。可能失败的校验、资源获取和状态加载放在 Started，并由应用设置初始化上限。Bindings 不依赖实例状态，nil/live 描述须一致；生成绑定沿用 [actorx 规则](https://github.com/go-aie/actorx/blob/main/docs/runtime.zh.md)，注册和接入细节见[架构](architecture.zh.md#注册与上下文接入)。

## 地址与一次激活

Addr 保留逻辑身份和代次状态，不保存业务对象或本地执行地址。`rt.Addr(type, id)` 只校验身份并创建句柄，不做 I/O，也不启动目标。省略 ID 立即生成新身份，显式空 ID 无效。每次 Addr 调用分配独立状态，值复制共享状态并可并发使用；`Key()` 返回身份副本，`IsZero()` 只表示是否初始化。完整名称规则见 [wire 契约](contract.zh.md#名称与规范身份)。

首次 Send/Call/Stream 选定已有代或争取创建新代，再等待就绪并执行；全部逻辑调用经过 NATS，包括目标位于同进程的情况。选中 STARTING 或 FAILED 不等于业务 READY。绑定后失败、退出或同 key 出现新代均不重绑。Stop/Wait 也固定原代，冷控制不创建，详见[生命周期](lifecycle.zh.md)。

调用完成、丢失地址和调用方 Close 都不结束远端对象；共享对象和临时对象的结束责任见[对象寿命](lifecycle.zh.md#对象寿命)。

## Actor 内寻址与 Self

handler、生命周期和串行任务完成回调仍接收 actorx.Context：

- `actorxdist.MustFrom(ctx).Addr(type, id)`：在所属 Runtime 中取得目标地址，业务结构体无需保存 Runtime。
- `actorxdist.MustFrom(ctx).Self()`：读取当前激活已经固定的回信地址，可放入普通消息供通知中心随后 Send。
- `dctx, ok := actorxdist.From(ctx)`：用于同一业务兼容纯本地执行；nil、typed-nil、纯核心 Context 返回 nil/false，MustFrom 缺少能力时诊断 panic。寻址及通信错误仍正常返回。

Self 在 Started 前已安装，关闭期间仍保留身份；无 I/O，成功不证明 READY 或存活。旧 Self 不会跟随新代。希望使用时选择当前代的业务应传 ActorKey，再重新取得 Addr。不在 Started 或串行 handler 中同步 Call/Stream/Wait 自己；自停使用 `ctx.Stop`。

WithContext(parent) 采用新 parent 的期限、取消和值，同时保留能力与 Self。上下文不跨请求保存；启动 `actorx.StartTask(ctx, ...)` 前取得所需地址，worker 使用任务 context 通信。

`ctx.Spawn` 创建本地实例并返回 actorx.Addr。子对象及后代有寻址能力但没有逻辑身份，Self 返回 actorx.ErrSetup；不继承父 generation，也不建立停止传播。逻辑对象通过 `dctx.Addr` 寻址，见 [Button → Counter](../examples/click/README.zh.md)。

## 地址传递

`json.Marshal(addr)` 导出 version/type/id 和可选 generation；未绑定时省略 generation。`rt.DecodeAddr(data)` 关联接收 Runtime 并创建独立状态，已有 generation 立即固定；不查询或激活目标。消息、结果和流 chunk 中的嵌套 Addr 由 codec 自动导入。跨进程传递地址不共享本地可变状态。

DecodeAddr 拒绝未知/重复字段、非法身份和显式空 generation；不支持的版本返回 ErrProtocol。默认 json.Unmarshal 缺少接收 Runtime，返回 actorx.ErrSetup。Runtime 关闭后 Addr/DecodeAddr 返回 ErrClosed，已有地址仍可读 Key 和导出身份。

掌握地址模型后，继续阅读[调用语义](runtime.zh.md)和[生命周期](lifecycle.zh.md)；想了解唯一归属如何成立，再查 [Registry 协议](registry.zh.md)。
