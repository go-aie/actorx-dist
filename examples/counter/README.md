# Counter across processes

English | [中文](README.zh.md) · [Module](../../README.md)

actors.go defines Add, Counter and CounterSpec; main.go handles flags, Runtime lifetime and two process roles: worker registers `rt.Register(ctx, CounterSpec)` and waits for shutdown; client registers no Actors and calls the remote Counter. The default role is client.

Start the [development NATS server](../../README.md), then run these commands from the actorx-dist repository root in separate terminals:

```sh
# Terminal 1: worker A; wait for "Counter worker ready".
go run ./examples/counter -role worker
# Terminal 2: worker B, sharing the same type and NATS account.
go run ./examples/counter -role worker
# Terminal 3: client; on a fresh ID this prints 5, then 10.
go run ./examples/counter -role client -id orders-42
# Another client process with the same ID prints 15, then 20.
go run ./examples/counter -role client -id orders-42
# A different ID has independent state: 5, then 10.
go run ./examples/counter -role client -id another
```

Send adds 2 and waits for mailbox admission; Call adds 3 and returns the accumulated value. Conditional KV ownership selects one worker for each key. Closing a client leaves the remote object alive. Ctrl-C gracefully closes each worker and joins its owned instances. All processes can use `-nats nats://SERVER:4222` to connect to the same service.

CounterSpec stays beside Counter; bindings are generated with `go generate ./examples/counter`. The client role still links this program's Actor definitions. Remote Stop/Wait is covered in [lifecycle](../../docs/lifecycle.md); continue with [Button → Counter](../click/README.md) for cross-process composition.
