# Click

[English](README.md) | 中文

[actors.go](actors.go) 定义共享的 `click` 包，包含 Button、Counter、各自的消息和 Spec。三个独立程序分别展示各自职责：

- [button/main.go](button/main.go) 注册 `click.ButtonSpec`，提供 Button。
- [counter/main.go](counter/main.go) 注册 `click.CounterSpec`，提供 Counter。
- [consumer/main.go](consumer/main.go) 调用 Button，不注册 Actor。

启动[开发 NATS 服务](../counter/README.zh.md#启动-nats)，再从 actorx-dist 仓库根目录，在三个独立终端运行：

```sh
# 终端 1：等待 "button provider ready"。
go run ./examples/click/button
# 终端 2：等待 "counter provider ready"。
go run ./examples/click/counter
# 终端 3：第一个调用方进程输出 1、2。
go run ./examples/click/consumer -id clicks-42
# 新的调用方进程使用相同 ID，输出 3、4。
go run ./examples/click/consumer -id clicks-42
```

consumer → Button 和 Button → Counter 均在独立进程间经过 NATS。Button 同时也是 Counter 的 consumer，使用 `actorxdist.MustFrom(ctx).Addr` 取得 Counter，并用原 actorx.Context 调用 Add。构造函数仅接收 ActorKey、保存目标 ID；不同 Button ID 对应独立的 Counter 状态。

consumer Close 后两个提供方进程中的对象继续存活；各 provider 收到 Ctrl-C 后 Close 并等待所属实例完整退出。各角色可通过 `-nats nats://SERVER:4222` 选择服务，通过 `go generate ./examples/click` 生成绑定。

三个程序均导入共享 Actor 包，各自管理 Runtime 的生命周期。长 I/O 和流使用 actorx.StartTask 及其任务 context，见[调用与流语义](../../docs/runtime.zh.md)。
