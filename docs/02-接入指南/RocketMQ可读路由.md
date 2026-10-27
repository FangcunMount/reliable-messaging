# RocketMQ 可读路由与历史证据

本接口为新增能力，尚未发布或部署。Qlume 的实现、CI、发布与上线进度分别维护于 [infra 执行总表](https://github.com/FangcunMount/infra/blob/main/docs/server/rocketmq-readable-names-execution.md)。

## 规则与配置

manifest v3 的活动路由必须指定 `naming_scheme: readable-v1`。Topic 使用 `场景--normal或tx--owner--route_revision`；Group 使用 `逻辑Topic--逻辑订阅`。输入只接受小写字母/数字及单个点号或连字符分隔；点号转换成连字符，结果最多64字节。非法、超限及归一化碰撞直接拒绝，不截断、不随机追加，也不回退为哈希。

Go 的 `PhysicalTopicV3` / `ConsumerGroupV3` 与 Python 的 `physical_topic_v3` / `consumer_group_v3` 共享[机器向量](../../contracts/rocketmq-routing-v3.json)。原v1/v2函数和既有命名保持不变。命名方案不改变事务的route revision、writer epoch、Reference codec、消息指纹或wire协议。

`ParseRouteManifest` / `parse_route_manifest` 校验版本及完整清单；`RouteResolver` 只能从宿主提供的固定配置构造。它不读取文件、不做网络I/O、不启动线程、不选择数据库：

- `PublishRoute` / `publish_route` 仅返回活动路由。
- `SubscriptionRoutes` / `subscription_routes` 仅返回活动物理输入，同一逻辑订阅必须共享Group；宿主原预算保持。
- `RecordedRoute` / `recorded_route` 按逻辑身份及物理名称精确核对活动或历史证据，不授予发布、订阅或执行资格。

`historical_routes` 是v3清单的只读旧v2映射。它与活动资源必须分离，不能由消息属性、URL或请求正文注入。普通Publisher和Subscriber只使用活动清单；历史清单不得注册到Broker。

## 事务发送与原事实

`TransactionRoute` 增加可选 `NamingScheme` / `naming_scheme`。`TransactionConfig.HistoricalRoutes` / `historical_routes` 只允许显式Replan验证前一份原计划；Start/Checker/Reserve/Retry/Execute仍仅使用活动路由。

已提交结果从原证据读取，不能因改名再次执行。Prepared/Unknown不能Replan；只有存储原子证明前一执行及分支为Aborted，宿主明确给出新operation和完整candidate时，Replan才可授予新执行。SDK不自动生成新计划、不补做业务、不重放历史消息。跨路由候选可以保留原消息和wire，计划摘要须反映新operation/物理Topic；旧计划和终态不能被覆盖。

真实Broker、原数据库事务、跨实例回查和生产迁移是独立门禁。当前单元测试证明codec、可信路由选择、预算和编排，不能代替真实依赖验收。
