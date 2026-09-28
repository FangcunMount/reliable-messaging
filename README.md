# reliable-messaging

独立、经过裁剪的 Go 可靠消息 SDK。宿主在原业务事务中持久化消息意图，并管理 Relay 的运行与停止；首期复用 NSQ。

**截至 2026-09-27：v0.2.1 已正式发布，M6 稳定化和消息能力归属迁移仍在进行。** M6 升级说明／隔离验证 PR #8 已合入 main；#9–#16 仍是未发布的连续草稿候选，包含历史 wire、通用事件目录、原始字节发布、显式 topic／channel 准备、Subscriber／失败中转和 SDK 自持有的 NSQ Producer。一次性 NSQ／MySQL 已覆盖首条发布早于消费者启动、订阅先于 topic 登记、稳定失败组接管、失败审计拒写后跨进程恢复、失败中转 PUB 接收后确认丢失，以及 lookupd 恢复后发现中断期间加入的节点。持续不可用期间发现新节点、宿主真实服务生命周期、业务幂等和生产恢复仍未验收。IAM、qs-server 有相应接入草稿，但现行生产镜像仍含旧组件路径；M6-04 的最终目标是由本 SDK 承载通用 MQ／消息能力，让 `component-base` 退出对应职责。完整验收按项目实施规划逐项核对，不能从代码包或隔离测试推断生产切换完成。

## 开发

使用 Go 1.25.9 或以上；验证工具链固定 Go 1.25.12，golangci-lint v2.5.0。

```sh
make check
make lint
make integration  # disposable local Docker resources
make subscription-integration  # focused disposable NSQ consumer proof
```

根目录单 module。只有实际需求出现时才创建适配器与公开包；当前 `tests/compatibility` 验证候选跨语言身份样例，`tests/architecture` 约束依赖边界。

当前已实现消息身份、原事务追加、带凭证的状态转移、受监督投递接缝与传输发布适配；`wire/legacy` 候选封装现行传输 envelope 和失败中转格式，`wire/domain` 候选封装其内部的通用事件 JSON 字段及元数据时间格式。NSQ 候选增加原始编码字节的直接发布入口、显式 topic／channel 准备，以及显式确认、有界失败中转、原 ID 保留、直接 nsqd／lookupd 订阅、SDK 管理的失败中转生产者与自持有的普通发布 Producer。`catalog` 候选承接旧组件的通用 YAML 目录校验和事件到 topic 查询，事件名称、路由配置和业务策略仍由宿主提供。IAM／QS 的显式发布订阅和服务持久审计接入已有未合并候选，生产迁移尚未完成。租户审批、业务重试授权、消费幂等、宿主迁移和上线操作仍属于各服务。

`outbox.StatusBucket`／`StatusSnapshot`／`StatusReader` 候选承接通用状态形状；状态名称、数据库读取及业务治理仍由宿主实现。它只描述持久意图的观察结果，不把 `published` 解释为消费者或业务完成。

## 可靠性边界

- 不新增中央消息数据库，Outbox 与业务事实使用宿主原事务。
- Broker 发布确认不代表消费者完成，更不自动解决 MySQL 双写。
- 传输重试保持消息身份，业务重试授权留宿主；未知外部调用不盲目重发。
- 历史未完成消息、wire、人工治理、冻结配置及回滚语义必须保护。IAM 已选择旧链路排空后使用标准表，标准表仍属于宿主原数据库。
- 双存储与故障验证在 M2；IAM、qs-server 切换分别在 M3/M5；Python 和 RabbitMQ 独立评估。

## 维护与分发

项目发起人最终审核 SDK、IAM 接入、qs-server 接入和运维。2026-09-23 仓库已设为 Public，Go module 可按固定版本公开下载；读取本模块不再需要 GitHub App、私库凭据或专门的 `GOPRIVATE` 配置。

仓库可见性变更不替代版本审核。开放源码许可证仍待项目发起人决定，本次不新增许可证；依赖仍遵守各自许可证。

真实隔离环境入口见 [integration](tests/integration/README.md)，可执行接入参考见 [examples](examples/README.md)。

MySQL 原事务可使用 UTC 或 UTC+8 驱动连接；标准表时间字段的显式存储约定及非 UTC 修正见 [MySQL 时间边界](storage/mysql/README.md)。

详见 [契约](contracts/README.md)、[开发规则](CONTRIBUTING.md) 与 [阶段状态](docs/status.md)。
