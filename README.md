# reliable-messaging

独立、经过裁剪的 Go 可靠消息 SDK。宿主在原业务事务中持久化消息意图，并管理 Relay 的运行与停止；首期复用 NSQ。

**截至2026-10-03：Go最新预发布版为v0.3.0-m6.4，v1.0尚未发布。** IAM主线及最后实际运行核验固定m6.1；本日实际读取QS API、两Collection和三Worker的二进制，均固定m6.4／component-base v0.8.0。通用消息类型／编码、Outbox／Relay、NSQ发布／订阅／topic及channel准备、失败结算与Redis signaling已由本SDK承载；受支持Go调用者已退出component-base旧消息包。m6.2的已确认原行条件重排仍需宿主审批／审计和原事务，m6.4的Redis提示仍为最佳努力。生产时间、代表负载／资源及压力回退等未闭合项不由组件退役或隔离CI代验。具体支持下限、精确已测组合、wire／状态及升级限制见[Go兼容策略](docs/compatibility.md)。

## Python 最小 SDK

Python 包位于 [`python/`](python/README.md)，发行名 `fangcun-reliable-messaging`，导入名
`reliable_messaging`。独立预发布版
[`python/v0.1.0a1`](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.1.0a1)
提供消息身份/hash、SQLAlchemy 原事务绑定、现有 Outbox 表适配、业务持久回执结算和显式异步生命周期。
支持 Python 3.11–3.13，已验证的事务绑定限于 MySQL/asyncmy；不包含 Python NSQ、业务执行或恢复状态机。
qs-ai 复用这些机制，保留原 gRPC 持久回执和同进程后台循环。Python 与 Go 独立版本发布；本节不改变上方 Go 阶段状态。

## 开发

SDK最低Go1.25.9，建议工具链1.25.12；两者默认检查均已实际运行，基础设施验证固定1.25.12，golangci-lint v2.5.0。其它工具链／平台的支持不由“或以上”自动推定，见兼容策略。

```sh
make check
make lint
make integration  # disposable local Docker resources
make subscription-integration  # focused disposable NSQ consumer proof
```

根目录单 module。只有实际需求出现时才创建适配器与公开包；当前 `tests/compatibility` 验证候选跨语言身份样例，`tests/architecture` 约束依赖边界。

当前已实现消息身份、原事务追加、带凭证的状态转移、受监督投递接缝与传输发布适配；`wire/legacy`封装历史传输envelope和失败中转格式，`wire/domain`封装通用事件JSON字段及元数据时间格式。NSQ适配器提供原始编码字节发布、topic／channel准备、显式确认、有界失败中转、原ID保留、直接nsqd／lookupd订阅与SDK自持有Producer。`catalog`承接通用YAML目录校验和事件到topic查询，事件名称、路由配置和业务策略仍由宿主提供。旧component-base通用消息实现已退役；宿主历史解码证据、业务治理与有效回退责任保留。租户审批、业务重试授权、消费幂等、宿主迁移和上线操作仍属于各服务。

`outbox.StatusBucket`／`StatusSnapshot`／`StatusReader` 承接通用状态形状；状态名称、数据库读取及业务治理仍由宿主实现。它只描述持久意图的观察结果，不把 `published` 解释为消费者或业务完成。

## 可靠性边界

- 不新增中央消息数据库，Outbox 与业务事实使用宿主原事务。
- Broker 发布确认不代表消费者完成，更不自动解决 MySQL 双写。
- 传输重试保持消息身份，业务重试授权留宿主；未知外部调用不盲目重发。
- 历史未完成消息、wire、人工治理、冻结配置及回滚语义必须保护。IAM 已选择旧链路排空后使用标准表，标准表仍属于宿主原数据库。
- 双存储与故障验证在 M2；IAM、qs-server 切换分别在 M3/M5；Python 最小 SDK 见上节，RabbitMQ 独立评估。

## 维护与分发

项目发起人最终审核 SDK、IAM 接入、qs-server 接入和运维。2026-09-23 仓库已设为 Public，Go module 可按固定版本公开下载；读取本模块不再需要 GitHub App、私库凭据或专门的 `GOPRIVATE` 配置。

仓库可见性变更不替代版本审核。开放源码许可证仍待项目发起人决定，本次不新增许可证；依赖仍遵守各自许可证。

真实隔离环境入口见 [integration](tests/integration/README.md)，可执行接入参考见 [examples](examples/README.md)。

MySQL 原事务可使用 UTC 或 UTC+8 驱动连接；标准表时间字段的显式存储约定及非 UTC 修正见 [MySQL 时间边界](storage/mysql/README.md)。

详见[身份样例及历史说明](contracts/README.md)、[Go兼容策略](docs/compatibility.md)、[开发规则](CONTRIBUTING.md)与[阶段状态](docs/status.md)。
