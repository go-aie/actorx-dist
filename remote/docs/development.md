# Remote development and releases

English | [中文](development.zh.md)

Run commands from the repository root. The root module's `./...` does not traverse the nested Go SDK module, so test it separately.

## Routine checks

```sh
GOWORK=off go test -race ./remote/...
(cd remote/sdk/go && GOWORK=off go test -race ./... && GOWORK=off go vet ./...)
PYTHONPATH=remote/sdk/python/src python3 -m unittest discover -s remote/sdk/python/tests
for vector in v1.json json-boundaries.json; do
  cmp "remote/protocol/testdata/$vector" "remote/sdk/go/testdata/$vector"
done
```

CI runs these checks; examples are for demonstration and have no CI gates. Core behavior belongs in the corresponding Go `_test.go` files and Python `tests/`. SDK tests read only module-local protocol vectors.

## Optional interoperability checks

Use the existing HTTP/NATS tests with an isolated JetStream service:

```sh
ACTORX_REMOTE_NATS_URL=nats://127.0.0.1:4222 \
  GOWORK=off go test -race ./remote/http -run NATS -v
```

Without this variable, real NATS tests are skipped. For Go/Python interoperability, use the [Counter](../examples/counter/README.md) or [Chat](../examples/chat/README.md) commands and choose each side's language as needed.

## Releases

Tag the SDK submodule with `remote/sdk/go/vX.Y.Z`. After publishing, use the actual version in an external project with `GOWORK=off` and no `replace`, then build and run a provider and consumer. The examples' `v0.0.0` and local `replace` are for source development only.

Verify unit behavior, real interoperability and released-module consumption separately. Source tests do not establish that a published release works.
