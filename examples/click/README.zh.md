# Click

[English](README.md) | 中文

actors.go 定义 Button、Counter、各自的消息和 Spec；main.go 负责参数、Runtime 生命周期及三个进程角色：button 仅注册 `rt.Register(ctx, ButtonSpec)`，counter 仅注册 `rt.Register(ctx, CounterSpec)`，client 不注册 Actor。默认角色为 client。

启动[开发 NATS 服务](../counter/README.zh.md#启动-nats)，再从 actorx-dist 仓库根目录，在三个独立终端运行：

```sh
# 终端 1：等待 "button worker ready"。
go run ./examples/click -role button
# 终端 2：等待 "counter worker ready"。
go run ./examples/click -role counter
# 终端 3：第一个调用进程输出 1、2。
go run ./examples/click -role client -id clicks-42
# 新的调用进程使用相同 ID，输出 3、4。
go run ./examples/click -role client -id clicks-42
```

client → Button 和 Button → Counter 均在独立进程间经过 NATS。Button 使用 `actorxdist.MustFrom(ctx).Addr` 取得 Counter，并用原 actorx.Context 调用 Add。构造函数仅接收 ActorKey、保存目标 ID；不同 Button ID 对应独立的 Counter 状态。

client Close 后两个工作进程中的对象继续存活；各 worker 收到 Ctrl-C 后 Close 并等待所属实例完整退出。各角色可通过 `-nats nats://SERVER:4222` 选择服务，通过 `go generate ./examples/click` 生成绑定。

client 角色仍链接共同程序中的 Actor 定义。长 I/O 和流使用 actorx.StartTask 及其任务 context，见[调用与流语义](../../docs/runtime.zh.md)。
