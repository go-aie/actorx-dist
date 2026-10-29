# Counter across processes

English | [中文](README.zh.md)

A provider registers the Counter type and keeps its state; a separate consumer sends messages by type and business ID. Run the consumer again to observe retained state, or start another provider to explore unique ownership.

## Start NATS

Requires Go 1.27 and [NATS Server v2.10.18](https://github.com/nats-io/nats-server/releases/tag/v2.10.18). Install the server for your platform and make sure `nats-server` is on your PATH. Run all commands below from the actorx-dist repository root.

In terminal 1, start NATS with the [development configuration](../nats.conf), which enables JetStream on local port 4222:

```sh
nats-server -c examples/nats.conf -sd /tmp/dist-counter
```

Keep this terminal running. The other distributed examples can use the same service. If you previously ran the single-entry examples, use a new development storage directory with `-sd`: moving the shared Go types into importable packages changes their contracts.

## Run Counter

### Provider

In terminal 2, start a provider and wait for `Counter provider ready`:

```sh
go run ./examples/counter/provider
```

### Consumer

In terminal 3, run the consumer:

```sh
go run ./examples/counter/consumer -id orders-42
```

For a new counter ID, the output is:

```text
5
10
```

The consumer runs two rounds, each using Send to add 2 and Call to add 3 and return the total. Run it again while the same provider is running to get 15, then 20: the counter retains its state between consumer runs. Send waits for mailbox admission; Call returns the accumulated business result.

## Multiple providers and IDs

Keep the original provider running and start a second provider in another terminal, sharing the same type and NATS account:

```sh
go run ./examples/counter/provider
```

In the consumer terminal, use a different ID:

```sh
go run ./examples/counter/consumer -id another
```

For a new ID, this prints 5, then 10. Different IDs have independent state. Conditional KV ownership selects one provider for each key; a new ID may belong to either provider. Calling `orders-42` again continues its existing count on the original owner.

## Shut down

The consumer exits automatically without stopping the counter. To finish, press Ctrl-C in each provider terminal and wait for its Runtime and owned actors to exit, then press Ctrl-C in terminal 1 to stop NATS.

State lives in provider memory and disappears on instance exit. Unreachable owners are not replaced automatically; see [explicit recovery](../../docs/deployment.md#explicit-recovery-and-storage-repair) after forced exit.

## How it works

[actors.go](actors.go) defines the shared `counter` package: Add, Counter and CounterSpec. The two programs import this package and own their Runtime lifetimes independently:

- [provider/main.go](provider/main.go) creates a Runtime, registers `counter.CounterSpec` and waits for shutdown.
- [consumer/main.go](consumer/main.go) creates a Runtime, obtains the address and sends Add messages without registering Actors.

Both accept `-nats nats://SERVER:4222`. Generate bindings with `go generate ./examples/counter`. The consumer also links the shared Actor definitions. Remote Stop/Wait is covered in [lifecycle](../../docs/lifecycle.md); continue with [Click](../click/README.md) for cross-process composition.

For the same counting behavior with a Python provider and HTTP consumer, see [Remote Counter](../../remote/examples/counter/README.md).
