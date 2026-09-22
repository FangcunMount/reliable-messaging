# reliable-messaging

独立、经过裁剪的 Go 可靠消息 SDK。宿主在原业务事务中持久化消息意图，并管理 Relay 的运行与停止；首期复用 NSQ。

**当前：M1 工程骨架，尚无可用于生产的 SDK API。** M0 已完成代码与部署基线分析。契约参考测试通过不代表 Outbox、事务适配或生产切换已经实现。

## 开发

使用 Go 1.25.9 或以上；验证工具链固定 Go 1.25.12，golangci-lint v2.5.0。

```sh
make check
make lint
```

根目录单 module。只有实际需求出现时才创建适配器与公开包；当前 `tests/compatibility` 验证候选跨语言身份样例，`tests/architecture` 约束依赖边界。

## 可靠性边界

- 不新增中央消息数据库，Outbox 与业务事实使用宿主原事务。
- Broker 发布确认不代表消费者完成，更不自动解决 MySQL 双写。
- 传输重试保持消息身份，业务重试授权留宿主；未知外部调用不盲目重发。
- 历史 schema/wire、人工治理、冻结配置及回滚语义必须保留。
- 双存储与故障验证在 M2；IAM、qs-server 切换分别在 M3/M5；Python 和 RabbitMQ 独立评估。

## 维护与分发

项目发起人最终审核 SDK、IAM 接入、qs-server 接入和运维。仓库保持私有，通过有权访问的 GitHub 账号分发 Go module；调用方配置 `GOPRIVATE=github.com/FangcunMount/*`，凭据由本机或 CI 管理，不写入仓库。

首期内部私有使用，不添加开放源码授权；对外分发与许可证另行决定。依赖仍遵守各自许可证。

详见 [契约](contracts/README.md)、[开发规则](CONTRIBUTING.md) 与 [阶段状态](docs/status.md)。
