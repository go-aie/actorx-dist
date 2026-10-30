# Cross-process streaming chat

English | [中文](README.zh.md)

Stream a simulated answer word by word from an Assistant in another process. No model or credentials are needed. The business flow matches the core [chat example](https://github.com/go-aie/actorx/blob/main/examples/chat/README.md); actorx-dist adds NATS transport, logical addressing and provider registration.

Requires Go 1.27+. Start the [development NATS server](../counter/README.md#start-nats), then run from the actorx-dist repository root in two separate terminals:

```sh
# Terminal 1: wait for "Assistant provider ready".
go run ./examples/chat/provider
# Terminal 2: stream from the remote Assistant.
go run ./examples/chat/consumer -id assistant-42
```

```text
answer: Actors use messages to coordinate work.
chunks: 6
```

[actors.go](actors.go) defines the shared `chat` package: Ask, TextChunk, Assistant and AssistantSpec.

- [provider/main.go](provider/main.go) creates a Runtime, registers `chat.AssistantSpec` and waits for shutdown.
- [consumer/main.go](consumer/main.go) creates a Runtime, obtains `Addr("chat.assistant", id)` and consumes TextChunk values through Stream without registering Actors.

Both consumer commands explicitly pass `-id assistant-42`, which is also the default; change it to select another logical Assistant. Both programs import the shared Actor package and manage their own Runtime lifetime.

The handler uses `actorx.StartTask` because `StreamReply.Write` can wait for a slow consumer. Its task writes chunks using the task context; the serial completion callback calls `Complete(err)`. The consumer prints each chunk as it arrives and prints the count only after Stream succeeds.

To watch incremental output with a slow consumer:

```sh
go run ./examples/chat/consumer -id assistant-42 -consume-delay 100ms
```

`-consume-delay` keeps each consumer loop body active for the given duration. The default six chunks exceed the four-chunk window, so continued delivery needs consumption credits. This demonstrates slow consumption; precise window limits and cancellation cleanup are checked by the focused Stream tests. See [Stream semantics](../../docs/runtime.md#stream).

The consumer request has a ten-second total deadline. Ctrl-C cancels it; cancellation requests remote work to stop and does not prove remote cleanup has completed. Closing the consumer leaves the remote Assistant alive. Ctrl-C on the provider closes its Runtime and waits for owned work. All roles accept `-nats nats://SERVER:4222` and must connect to the same service/account.

After changing a handler, run `go generate ./examples/chat`.
