# Counter across processes

English | [中文](README.zh.md)

A worker keeps the counter state; a separate client sends messages by type and business ID. Run the client again to observe retained state, or start another worker to explore unique ownership.

## Start NATS

Requires Go 1.27 and [NATS Server v2.10.18](https://github.com/nats-io/nats-server/releases/tag/v2.10.18). Install the server for your platform and make sure `nats-server` is on your PATH. Run all commands below from the actorx-dist repository root.

In terminal 1, start NATS with the [development configuration](../nats.conf), which enables JetStream on local port 4222:

```sh
nats-server -c examples/nats.conf -sd /tmp/dist-counter
```

Keep this terminal running. The other distributed examples can use the same service.

## Run Counter

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

The client runs two rounds, each using Send to add 2 and Call to add 3 and return the total. Run it again while the same worker is running to get 15, then 20: the counter retains its state between client runs. Send waits for mailbox admission; Call returns the accumulated business result.

## Multiple workers and IDs

Keep the original worker running and start a second worker in another terminal, sharing the same type and NATS account:

```sh
go run ./examples/counter -role worker
```

In the client terminal, use a different ID:

```sh
go run ./examples/counter -role client -id another
```

For a new ID, this prints 5, then 10. Different IDs have independent state. Conditional KV ownership selects one worker for each key; a new ID may belong to either worker. Calling `orders-42` again continues its existing count on the original owner.

## Shut down

The client exits automatically without stopping the counter. To finish, press Ctrl-C in each worker terminal and wait for its Runtime and owned actors to exit, then press Ctrl-C in terminal 1 to stop NATS.

State lives in worker memory and disappears on instance exit. Unreachable owners are not replaced automatically; see [explicit recovery](../../docs/deployment.md#explicit-recovery-and-storage-repair) after forced exit.

## How it works

[actors.go](actors.go) defines Add, Counter and CounterSpec; [main.go](main.go) handles flags, Runtime lifetime and two process roles: worker registers `rt.Register(ctx, CounterSpec)` and waits for shutdown; client registers no Actors and calls the remote Counter. The default role is client. All processes can use `-nats nats://SERVER:4222` to connect to the same service.

CounterSpec stays beside Counter; bindings are generated with `go generate ./examples/counter`. The client role still links this program's Actor definitions. Remote Stop/Wait is covered in [lifecycle](../../docs/lifecycle.md); continue with [Click](../click/README.md) for cross-process composition.

For the same counting behavior with a Python provider and HTTP consumer, see [Remote Counter](../../remote/examples/counter/README.md).
