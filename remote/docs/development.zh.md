# Remote 维护与发布

[English](development.md) | 中文

以下命令从仓库根目录执行。根模块的 `./...` 不进入嵌套的 Go SDK 模块，需单独测试 SDK。

## 日常检查

```sh
GOWORK=off go test -race ./remote/...
(cd remote/sdk/go && GOWORK=off go test -race ./... && GOWORK=off go vet ./...)
PYTHONPATH=remote/sdk/python/src python3 -m unittest discover -s remote/sdk/python/tests
for vector in v1.json json-boundaries.json; do
  cmp "remote/protocol/testdata/$vector" "remote/sdk/go/testdata/$vector"
done
```

CI 执行这些检查；示例仅用于演示，不设置 CI 门禁。核心行为放在对应的 Go `_test.go` 和 Python `tests/` 中；SDK 测试只读取模块内的协议向量副本。

## 按需联调

真实 HTTP/NATS 测试沿用已有入口，需要自行提供隔离的 JetStream 服务：

```sh
ACTORX_REMOTE_NATS_URL=nats://127.0.0.1:4222 \
  GOWORK=off go test -race ./remote/http -run NATS -v
```

未设置该变量时，真实 NATS 测试跳过。Go/Python 互通直接使用 [Counter](../examples/counter/README.zh.md) 或 [Chat](../examples/chat/README.zh.md) 的运行命令，按需选择两端语言。

## 发布

SDK 子模块使用 `remote/sdk/go/vX.Y.Z` tag。发布后，在仓库外以 `GOWORK=off`、无 `replace` 的项目依赖实际版本，编译并运行 provider/consumer。示例中的 `v0.0.0` 和本地 `replace` 仅用于源码开发。

单元测试、真实互通和发行包消费分别验证；源码测试通过不代表发行包已验证。
