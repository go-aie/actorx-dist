# Actorx Distributed

English | [中文](README.zh.md)

Actorx Distributed is a distributed actor framework for Go built on [Actorx](https://github.com/go-aie/actorx) and [NATS](https://nats.io/). It lets applications access stateful actors across processes by type and business ID, creating instances on demand.

Actorx handles actor execution within each process, NATS carries messages between processes, and JetStream KV coordinates instance ownership. Actors support Send, Call and Stream for one-way messages, request/reply and streaming results.

## Get started

A distributed counter stores a count and returns the new total after each Add message. The snippets below show the main APIs; see the [Counter example](examples/counter/README.md) for a complete runnable program with generated bindings, error handling and shutdown.

```go
import (
    "github.com/go-aie/actorx"
    "github.com/go-aie/actorx-dist"
)

type Add struct {
    N int `json:"n"`
}

//actorx:actor
type Counter struct {
    count int
}

func (c *Counter) Add(_ actorx.Context, msg Add) (int, error) {
    c.count += msg.N
    return c.count, nil
}

var CounterSpec = actorxdist.ActorSpec[*Counter]{
    Type: "counter",
    New: func(actorxdist.ActorKey) *Counter {
        return &Counter{}
    },
}
```

`Counter` defines the behavior; `CounterSpec` declares its type and constructor. A provider registers the spec and keeps running to handle requests. On shutdown, `Runtime.Close` removes its registrations and waits for its actors to exit:

```go
ctx := context.Background()
rt, _ := actorxdist.NewRuntime(ctx)
defer rt.Close(ctx)
_, _ = rt.Register(ctx, CounterSpec)
```

A separate consumer creates its own Runtime and calls the counter by type and ID, without registering any actors:

```go
rt, _ := actorxdist.NewRuntime(ctx)
defer rt.Close(ctx)

addr, _ := rt.Addr("counter", "orders-42")
n, _ := addr.Call[int](ctx, Add{N: 5})
fmt.Println(n) // 5 for a new counter
```

`Addr("counter", "orders-42")` creates a logical address without starting an actor. The first Send, Call or Stream finds or activates the instance; the framework ensures unique ownership for each type and ID.

State lives in provider memory and disappears on instance exit. Unreachable owners are not replaced automatically; see [explicit recovery](docs/deployment.md#explicit-recovery-and-storage-repair) after forced exit.

## Examples

Start with Counter for environment setup, run commands, expected output and shutdown. All examples can share the same NATS service.

| Example | What it teaches |
| --- | --- |
| [Counter](examples/counter/README.md) (start here) | Cross-process calls, retained state and multiple providers. |
| [Click](examples/click/README.md) | Button calls Counter to compose actors across processes. |
| [Chat](examples/chat/README.md) | Cross-process Stream and incremental output with a slow consumer. |

## Learn more

| Guide | Contents |
| --- | --- |
| [Concepts](docs/concepts.md) | Logical objects, activation generations, addresses and contexts. |
| [Call semantics](docs/runtime.md) | Send/Call/Stream, deadlines, errors and capacity. |
| [Lifecycle](docs/lifecycle.md) | Stop/Wait, object lifetime and cleanup responsibility. |
| [Deployment](docs/deployment.md) | Configuration, permissions, contract upgrades and explicit recovery. |
| [Cross-language access](remote/README.md) | HTTP/SSE Call/Stream, Python providers and consumers, and the remote protocol. |

For implementation details, see [architecture](docs/architecture.md), [wire contract and limits](docs/contract.md), the [Registry protocol](docs/registry.md), and the [NATS reference](docs/nats.md) for namespaces, subjects and permissions.
