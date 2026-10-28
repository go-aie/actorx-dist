# Client → Button → Counter

English | [中文](README.zh.md) · [Concepts](../../docs/concepts.md)

actors.go defines Button, Counter, their messages and their Specs; main.go handles flags, Runtime lifetime and three process roles: button registers only `rt.Register(ctx, ButtonSpec)`, counter registers only `rt.Register(ctx, CounterSpec)`, and client registers no Actors. The default role is client.

Start the [development NATS server](../../README.md), then run from the actorx-dist repository root in three separate terminals:

```sh
# Terminal 1: wait for "button worker ready".
go run ./examples/click -role button
# Terminal 2: wait for "counter worker ready".
go run ./examples/click -role counter
# Terminal 3: first client process prints 1, then 2.
go run ./examples/click -role client -id clicks-42
# A new client process with the same ID prints 3, then 4.
go run ./examples/click -role client -id clicks-42
```

Both client → Button and Button → Counter traverse NATS between independent processes. Button obtains Counter with `actorxdist.MustFrom(ctx).Addr` and calls Add using the original actorx.Context. Its constructor receives only ActorKey and retains the target ID. A different Button ID selects independent Counter state.

Client Close leaves both workers' objects alive. Ctrl-C closes each worker and joins its owned instances. Use `-nats nats://SERVER:4222` on all roles to select another service. Generate bindings with `go generate ./examples/click`.

The client role still links the shared program's Actor definitions. Long I/O and streaming use actorx.StartTask and its task context; see [call and stream semantics](../../docs/runtime.md).
