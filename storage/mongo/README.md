# Mongo 标准 Outbox 适配器

本包提供原事务 Appender 与标准租约 Store，使用 [go.mod](../../go.mod)固定的 mongo-driver v1。client、事务、持久性、集合、索引和生命周期由宿主拥有。用法见 [Go 接入](../../docs/02-接入指南/Go接入.md)，完整状态与失败窗口见[事务与 Outbox](../../docs/01-核心设计/事务与Outbox.md)。

## 原事务绑定

传入现有 WithTransaction callback 的 SessionContext 及同一 client collection。在 callback 外准备原 ID、payload/密文和 due；每次重入重新 Bind。adapter 不开始、提交或重试事务。

SessionContext 本身不证明活动事务。[appender.go](appender.go)检查 underlying session 的 running state；v1 只能通过 deprecated/不稳定 mongo.XSession 暴露它，该依赖局限于 adapter。缺失/未开始/已结束 scope 拒绝，驱动升级须重验；同一 session 不能并发使用。

标准 document 的_id 是身份 tuple。相同身份/内容追加幂等，冲突返回 outbox.ErrConflict 并要求宿主中止事务。它不是 QS 历史 document，SDK 不自动映射旧字段/token。

## Store、索引与时间

[New与Indexes](store.go)借用 collection；宿主在业务事务外显式安装索引，核对事务部署和真实查询计划。$$NOW 过滤的索引表现不能只从静态定义推断。

领取使用原子 FindOneAndUpdate、数据库$$NOW 与 token/version/state/expiry 栅栏。Claim.RecordID 为不透明 BSON 身份编码，不可按 MySQL 十进制解析。租约精度毫秒；部分领取失败留下的已领取行由租约恢复，意图不丢弃。

新行有独立 created_at/updated_at，原事件时间不代表队列年龄。旧缺 failure_count 读取为零；成功栅栏 Retry/Quarantine 原子递增，reclaim/Confirm 不增加。旧缺 created_at 可读取，但宿主治理/公平扫描仍需分类或排空，Store 可读不等于完整宿主兼容。

Mongo 保存实际 instant、UTC+8 用于显示；immutable occurred_at 不改写。升级边界见[兼容与升级](../../docs/03-维护与验证/兼容与升级.md)。

## 验证边界

[mongo_test.go](../../tests/integration/mongo_test.go)使用一次性真实 replica set 证明 commit/rollback、callback 重入、原字节和冲突。重入由实际写入后的 labeled callback error 诱发，不能冒充真实 election/network 故障。

另一个隔离 failpoint 注入 unknown commit，证明 driver 重复 commit 且不重跑业务 callback。这些 failpoint 不能在生产启用。索引效率、保留策略和宿主生产恢复另验，入口见[测试与故障验证](../../docs/03-维护与验证/测试与故障验证.md)。
