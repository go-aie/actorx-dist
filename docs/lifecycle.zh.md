# 生命周期

[English](lifecycle.md) | 中文

请求完成与对象退出是两个边界。Send/Call/Stream 的结束规则见[调用语义](runtime.zh.md)；实例完整退出还须结束 handler、任务及资源收尾，再提交分布式终态。业务的 Started/Stopping/Stopped 规则沿用 [actorx 生命周期](https://github.com/go-aie/actorx/blob/main/docs/lifecycle.zh.md)。

## 对象寿命

| 场景 | 结束责任 |
| --- | --- |
| 多调用方共享对象 | 应用约定停止时机；一次请求或调用方关闭不代表共享对象不再需要 |
| 独占临时对象 | 保留原 Addr，用独立收尾期限 Stop → Wait，再关闭调用方 Runtime |
| Actor 间协作 | 地址和调用不建立停止传播；专属依赖由应用显式结束 |
| 撤销类型注册 | Registration.Unregister 关闭该份新激活能力，停止并结清所属逻辑实例；见[注册与撤销](runtime.zh.md#注册与撤销) |
| 所属提供方进程退出 | Runtime.Close 结清全部所属代，包括保留的失败激活和本地子对象 |

丢失 Addr、结束调用、关闭调用方或停止发起者不自动停止远端对象。省略 ID 后通信会访问新对象，当前没有闲置回收。Self 也不提供自动退订或级联停止。

## Stop 与 Wait

| 操作 | 成功含义 |
| --- | --- |
| `addr.Stop(cause)` | 本代新受理已关闭；不等待 handler、任务或 Stopped。首次受理的原因保留，重复 Stop 不覆盖 |
| `addr.Wait(ctx)` | 本代清理及所需终态提交完成，返回 actorx.Exit；Exit.Reason 是最终原因，独立 error 表示等待或观察失败 |

Stop 使用两秒控制期限；Wait 无 deadline 时默认五秒，显式期限最多六十秒。Wait 超时可再次等待，不取消实例退出或提交。

```go
// 独占临时对象：用原地址及独立期限收尾，随后才关闭调用方。
cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := addr.Stop(nil); err != nil {
    return err
}

exit, err := addr.Wait(cleanup)
// 检查 err；err == nil 时通过 exit.Reason 读取最终原因。
```

冷句柄 Stop/Wait 只查当前代，不创建对象；无当前代返回 ErrNotActivated。已有地址始终控制原代，复制和消息往返也不改变它。正常 Wait 后新地址可激活下一代，旧 Stop 不影响新代。旧代通信返回 ErrStopped；终态缺失或过期返回 ErrExitUnknown，原拥有者失联可返回 ErrOwnerUnavailable/ErrActivationUnknown。

即使首次业务调用失败，也保留原地址核实和收尾；Stop/Wait 失败不能记作清理完成。每个共享地址状态只缓存该代已提交 Exit，不保留实例；缓存结果在观察方 Runtime.Close 后仍可读取，无缓存时 Wait 返回 ErrClosed。

## 失败激活

New nil/panic、绑定契约不一致或 Spawn/Started 失败，须完整回滚后才提交失败终态。此时 **Wait 可成功观察失败，但 key 和代预算仍被保留**，不会自动重新构造；Stop、所属 Registration.Unregister 或拥有者 Runtime.Close 才释放。失败激活的 Stop 须等释放提交后成功，原原因不变；缓存的失败 Exit 也不证明已释放。

普通业务 error 只结束请求；panic、自停及显式 Stop 触发实例退出。完整状态与提交证据见 [Registry 协议](registry.zh.md#记录与条件操作)。

## Runtime.Close

Close 同步关闭本 Runtime 新调用及创建受理，随后在后台停止并等待所属实例、本地后代、在途工作和终态提交，最后关闭管理与网络资源。纯调用方 Close 不停止远端对象。

Close 超时只结束本次等待，后台继续清理，后续 Close 可以再次等待。未返回的应用回调继续占预算；KV 不可写时保留提交所需的证据、工作和连接，修复后才能结清。不把本地清理完成或一次超时当作全部关闭成功。

内部资源关闭顺序见[架构参考](architecture.zh.md#关闭与故障交接)。进程崩溃后须先隔离原拥有者，再按[部署流程](deployment.zh.md#显式恢复与存储修复)恢复归属；缺少证据时不能伪造 Exit。
