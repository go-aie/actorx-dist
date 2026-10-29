# Click

English | [中文](README.zh.md)

[actors.go](actors.go) defines the shared `click` package containing Button, Counter, their messages and Specs. Three independent programs show each responsibility:

- [button/main.go](button/main.go) provides Button by registering `click.ButtonSpec`.
- [counter/main.go](counter/main.go) provides Counter by registering `click.CounterSpec`.
- [consumer/main.go](consumer/main.go) calls Button without registering Actors.

Start the [development NATS server](../counter/README.md#start-nats), then run from the actorx-dist repository root in three separate terminals:

```sh
# Terminal 1: wait for "button provider ready".
go run ./examples/click/button
# Terminal 2: wait for "counter provider ready".
go run ./examples/click/counter
# Terminal 3: first consumer process prints 1, then 2.
go run ./examples/click/consumer -id clicks-42
# A new consumer process with the same ID prints 3, then 4.
go run ./examples/click/consumer -id clicks-42
```

Both consumer → Button and Button → Counter traverse NATS between independent processes. Button is also a consumer of Counter. It obtains Counter with `actorxdist.MustFrom(ctx).Addr` and calls Add using the original actorx.Context. Its constructor receives only ActorKey and retains the target ID. A different Button ID selects independent Counter state.

Consumer Close leaves both providers' objects alive. Ctrl-C closes each provider and joins its owned instances. Use `-nats nats://SERVER:4222` on all roles to select another service. Generate bindings with `go generate ./examples/click`.

All three programs import the shared Actor package and manage their own Runtime lifetime. Long I/O and streaming use actorx.StartTask and its task context; see [call and stream semantics](../../docs/runtime.md).
