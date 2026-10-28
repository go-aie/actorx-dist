# Counter 跨进程调用

[English](README.md) | 中文 · [模块](../../README.zh.md)

actors.go 定义 Add、Counter 和 CounterSpec；main.go 负责参数、Runtime 生命周期及两个进程角色：worker 通过 `rt.Register(ctx, CounterSpec)` 注册创建能力并等待退出；client 不注册 Actor，只调用远端 Counter。默认角色为 client。

先启动[开发 NATS 服务](../../README.zh.md)，再从 actorx-dist 仓库根目录，在不同终端运行：

```sh
# 终端 1：工作进程 A，等待 "Counter worker ready"。
go run ./examples/counter -role worker
# 终端 2：工作进程 B，使用相同类型及 NATS account。
go run ./examples/counter -role worker
# 终端 3：调用进程，新 ID 依次输出 5、10。
go run ./examples/counter -role client -id orders-42
# 另一个调用进程使用相同 ID，输出 15、20。
go run ./examples/counter -role client -id orders-42
# 不同 ID 的状态独立，输出 5、10。
go run ./examples/counter -role client -id another
```

Send 增加 2，等待 mailbox 受理；Call 再增加 3，返回累计值。KV 条件写为同一个 key 确定唯一拥有者。关闭 client 不停止远端对象；各 worker 收到 Ctrl-C 后 Close 并等待所属实例完整退出。所有进程可通过 `-nats nats://SERVER:4222` 连接同一服务。

CounterSpec 与 Counter 一起定义，通过 `go generate ./examples/counter` 生成绑定。client 角色仍链接本程序中的 Actor 定义。远端 Stop/Wait 见[生命周期](../../docs/lifecycle.zh.md)，跨进程组合调用见 [Button → Counter](../click/README.zh.md)。

相同计数业务的 Python provider 与 HTTP consumer 接入方式见 [Remote Counter](../../remote/examples/counter/README.zh.md)。
