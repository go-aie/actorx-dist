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
  GOWORK=off go test -race ./remote/... -run 'NATS|RemoteBridgeFingerprint' -v
```

未设置该变量时，真实 NATS 测试跳过。Go/Python 互通直接使用 [Counter](../examples/counter/README.zh.md) 或 [Chat](../examples/chat/README.zh.md) 的运行命令，按需选择两端语言。

目录验收由 `TestDirectoryHTTPClusterNATS` 覆盖三个 Runtime、多 Gateway、空目录、撤销、原生类型过滤、描述冲突、无参数集群发现、拒绝 scope/window 参数和完整报文 8 KiB 拒绝；`TestRemoteBridgeFingerprint` 将固定协议映射与真实 Runtime 契约核对。桥接 DTO、绑定或核心 schema 变化时，必须重跑指纹门禁并评估 Remote 版本，不能仅替换常量。核心快照截断返回 1001/full，并验证成功结果只有 actors；SDK 的固定向量、非法目录结果及查询取消/关闭测试在日常门禁内。目录可见性不作为跨 Gateway 调用的验收证据。

## 发布

SDK 子模块使用 `remote/sdk/go/vX.Y.Z` tag。发布后，在仓库外以 `GOWORK=off`、无 `replace` 的项目依赖实际版本，编译并运行 provider/consumer。示例中的 `v0.0.0` 和本地 `replace` 仅用于源码开发。

单元测试、真实互通和发行包消费分别验证；源码测试通过不代表发行包已验证。

Stream 迭代器迁移保留 `actorx.remote/v1`，但破坏性替换 Go 消费 API，并改变有效生产者失败后的排空行为；Gateway 与两种 SDK 应一起升级。示例的本地 replace 只证明当前源码消费，模块代理和 Python 包索引的可获取性需要另行验证。固定的 actorx 版本应使用全新模块缓存与校验和验证；待对应根模块/SDK 版本发布后，再在无任何 replace 的外部应用中重跑构建。
