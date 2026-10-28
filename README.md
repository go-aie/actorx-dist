# Actorx Distributed

English | [中文](README.zh.md)

Actorx Distributed (Go package `actorxdist`) is a distributed Actor framework for Go built on [actorx](https://github.com/go-aie/actorx/blob/main/README.md). It provides logical addressing, on-demand activation, unique ownership, cross-process Send/Call/Stream, and lifecycle management through complete shutdown.

Business code keeps actorx stateful objects, serial message handling, asynchronous tasks and streaming results. dist uses NATS and JetStream KV for communication and ownership so callers access objects by type and business ID.

## Get started

A distributed counter keeps its count in an Actor. An Add message increments it and returns the new total.

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

Counter defines business behavior; CounterSpec declares its logical type and constructor. A worker registers the Spec and stays running to handle requests. Runtime.Close withdraws its registrations and joins its instances. Routine error handling and waiting for shutdown are omitted here:

```go
ctx := context.Background()
worker, _ := actorxdist.NewRuntime(ctx)
defer worker.Close(ctx)
_, _ = worker.Register(ctx, CounterSpec)
```

A separate caller creates its own Runtime without registering Actors. The following assumes a request context ctx and a previously unused target ID:

```go
client, _ := actorxdist.NewRuntime(ctx)
defer client.Close(ctx)

addr, _ := client.Addr("counter", "orders-42")
n, _ := addr.Call[int](ctx, Add{N: 5})
fmt.Println(n) // 5
```

`Addr("counter", "orders-42")` only obtains a logical handle. The first call finds or activates the object, with unique ownership for the logical identity. Once selected, that address stays on the same generation. Obtain a fresh address to select another generation after exit.

Inside an Actor, use `actorxdist.MustFrom(ctx).Addr(type, id)` for addressing and Self for a reply address on the current generation; see [concepts](docs/concepts.md). These snippets require generated Bindings. Complete definitions, bindings and process entry points are in the [Counter example](examples/counter/README.md).

## Run the example

Requires Go 1.27. Run from the actorx-dist repository root; go.mod pins a published actorx core version.

### Start NATS

Download the archive for your OS and architecture from the [official NATS v2.10.18 release](https://github.com/nats-io/nats-server/releases/tag/v2.10.18). For macOS Apple Silicon, download, extract and add the executable to this terminal's PATH as follows. Other platforms can select their matching package on the same release page:

```sh
curl -fL -o /tmp/nats-server.tar.gz \
  https://github.com/nats-io/nats-server/releases/download/v2.10.18/nats-server-v2.10.18-darwin-arm64.tar.gz
tar -xzf /tmp/nats-server.tar.gz -C /tmp
export PATH="/tmp/nats-server-v2.10.18-darwin-arm64:$PATH"
```

In terminal 1, enter the actorx-dist repository root and start the development server. Its configuration enables JetStream and listens on local port 4222. The storage directory must be writable:

```sh
nats-server -c examples/nats.conf -sd /tmp/dist-counter
```

Leave terminal 1 running. The examples below share this NATS service.

### Run Counter

Open terminal 2, enter the actorx-dist repository root and start a worker. Wait for `Counter worker ready`:

```sh
go run ./examples/counter -role worker
```

Open terminal 3, enter the actorx-dist repository root and run the client:

```sh
go run ./examples/counter -role client -id orders-42
```

With a previously unused ID, the output is:

```text
5
10
```

Each client round sends an increment of two, then calls an increment of three and reads the total. It runs two rounds. While the same worker stays alive, repeat the command in terminal 3 to print 15, then 20: the client process exits, but the remote Counter's state remains in the worker.

### Shut down

The client exits automatically after its calls, leaving the remote Counter alive. To finish, press Ctrl-C in terminal 2 and wait for the worker and its owned instances to exit. Then press Ctrl-C in terminal 1 to stop NATS.

State lives in worker memory and disappears on instance exit. Unreachable owners are not replaced automatically; see [explicit recovery](docs/deployment.md#explicit-recovery-and-storage-repair) after forced exit.

### More examples

Keep the NATS service running or restart it before continuing. Each example's README lists its processes and commands:

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

Implementation details live in [architecture and core integration](docs/architecture.md), [wire contract and limits](docs/contract.md), and the [Registry protocol](docs/registry.md). Released-version consumption, physical cross-host networking and replication remain unverified.
