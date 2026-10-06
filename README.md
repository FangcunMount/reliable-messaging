# reliable-messaging

嵌入宿主服务的可靠消息 SDK，提供 Go 和 Python 包。它将消息身份、事务内追加、Outbox、投递与确认机制集中维护；业务事务、消费幂等、业务回执、重试许可和运行资源仍由宿主负责。当前 Broker 适配为 NSQ。

Outbox 留在宿主业务数据库内，与业务事实使用同一笔事务。SDK 不需要独立部署，也不需要自己的 MySQL、MongoDB 或 Redis；启用某个适配器时，由宿主提供对应资源。Broker 确认、消费 FIN 和业务持久回执是不同的确认层次。

## 从哪里开始

- 理解方案：[文档导航](docs/README.md) → [项目定位与架构](docs/00-总览/项目定位与架构.md)。
- 接入项目：[Go 接入](docs/02-接入指南/Go接入.md)、[Python 接入](docs/02-接入指南/Python接入.md)、[宿主接入边界](docs/02-接入指南/宿主接入边界.md)。
- 选择版本：[发布索引](docs/releases/README.md)与[兼容与升级](docs/03-维护与验证/兼容与升级.md)。版本制品与宿主生产采用分别核验。
- 排查消息：[观测与故障处置](docs/03-维护与验证/观测与故障处置.md)。

Go module 为 `github.com/FangcunMount/reliable-messaging`，使用发布索引中的固定版本。Python 包为 `fangcun-reliable-messaging`，导入名为 `reliable_messaging`，安装和最小用法见 [Python 包入口](python/README.md)。Go 与 Python 独立发布，不能从一个语言的版本号推断另一种语言的能力。

## 能力与责任

| SDK 提供 | 宿主提供 |
|---|---|
| 原身份与字节校验、通用 wire 与目录机制 | 事件名、业务正文、topic/scope 映射、认证准入 |
| 原事务 Appender、租约型与回执型 Outbox 机制 | 业务连接、同事务业务写入、迁移与历史意图处理 |
| Relay、NSQ 发布/订阅/准备、失败中转 | 显式启动/监督/停止、消费幂等、持久接单与回执 |
| 通用状态与观察接口、最佳努力 Redis 提示 | 指标阈值、告警渠道、恢复权限、生产发布与验收 |

传输重投保持原身份与原字节。SDK 不根据超时推断模型调用失败，不替宿主授权业务重试，也不改写冻结配置。不同 Outbox 的状态和结算边界见[事务与 Outbox](docs/01-核心设计/事务与Outbox.md)。

## 开发与文档维护

```sh
make docs-check
make check
make lint
make integration              # 独立本机 Docker 资源，含公开 API 事务示例
make subscription-integration # 聚焦 NSQ 消费结算
```

检查层次和所需资源见[测试与故障验证](docs/03-维护与验证/测试与故障验证.md)，可执行参考见 [examples](examples/README.md)。修改契约时同步唯一正文，规则见[文档维护约定](docs/CONTRIBUTING-DOCS.md)和[贡献说明](CONTRIBUTING.md)。

本仓库公开可读；开放源码许可证尚待项目发起人决定。SDK、宿主接入、运维及正式发布按项目审核约定处理。历史阶段材料保留在[归档](docs/_archive/README.md)，不作为当前能力或生产状态的依据。
