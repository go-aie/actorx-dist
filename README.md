# Actorx Distributed

English | [中文](README.zh.md)

Actorx Distributed is a distributed actor framework for Go built on [Actorx](https://github.com/go-aie/actorx/blob/main/README.md) and [NATS](https://nats.io/). It lets applications access stateful actors across processes by type and business ID, creating instances on demand.

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

`Counter` defines the behavior; `CounterSpec` declares its type and constructor. A worker registers the spec and keeps running to handle requests. On shutdown, `Runtime.Close` removes its registrations and waits for its actors to exit:

```go
ctx := context.Background()
worker, _ := actorxdist.NewRuntime(ctx)
defer worker.Close(ctx)
_, _ = worker.Register(ctx, CounterSpec)
```

A separate client creates its own Runtime and calls the counter by type and ID, without registering any actors:

```go
client, _ := actorxdist.NewRuntime(ctx)
defer client.Close(ctx)

addr, _ := client.Addr("counter", "orders-42")
n, _ := addr.Call[int](ctx, Add{N: 5})
fmt.Println(n) // 5 for a new counter
```

`Addr("counter", "orders-42")` creates a logical address without starting an actor. The first Send, Call or Stream finds or activates the instance; the framework ensures unique ownership for each type and ID.

## Run the example

Requires Go 1.27 and [NATS Server v2.10.18](https://github.com/nats-io/nats-server/releases/tag/v2.10.18). Install the server for your platform and make sure `nats-server` is on your PATH. Run all commands below from the actorx-dist repository root.

### Start NATS

In terminal 1, start NATS with the development configuration, which enables JetStream on local port 4222:

```sh
nats-server -c examples/nats.conf -sd /tmp/dist-counter
```

Keep this terminal running for the examples below.

### Run Counter

In terminal 2, start a worker and wait for `Counter worker ready`:

```sh
go run ./examples/counter -role worker
```

In terminal 3, run the client:

```sh
go run ./examples/counter -role client -id orders-42
```

For a new counter ID, the output is:

```text
5
10
```

The client runs two rounds, each using Send to add 2 and Call to add 3 and return the total. Run it again while the same worker is running to get 15, then 20: the counter retains its state between client runs.

### Shut down

The client exits automatically without stopping the counter. To finish, press Ctrl-C in terminal 2 and wait for the worker and its actors to exit, then press Ctrl-C in terminal 1 to stop NATS.

State lives in worker memory and disappears on instance exit. Unreachable owners are not replaced automatically; see [explicit recovery](docs/deployment.md#explicit-recovery-and-storage-repair) after forced exit.

## More examples

Each example uses the same NATS service and includes its own run instructions:

| Example | What it teaches |
| --- | --- |
| [Counter](examples/counter/README.md) | Multiple workers, unique ownership for one ID and independent state for different IDs. |
| [Client → Button → Counter](examples/click/README.md) | Actor composition across independent processes. |
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
