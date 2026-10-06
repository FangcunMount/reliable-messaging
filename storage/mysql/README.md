# MySQL 标准 Outbox 适配器

本包实现标准租约 Outbox 的 Appender 与 Store。宿主拥有业务事务、池、session 时区、迁移和部署；Bind/BindGORM 不另开事务、不改 time_zone、不提交/回滚。用法见 [Go 接入](../../docs/02-接入指南/Go接入.md)，通用合同见[事务与 Outbox](../../docs/01-核心设计/事务与Outbox.md)。

## 原事务与 schema

- [Bind](store.go) 借用原 `*sql.Tx`；[BindGORM](gorm.go) 从受支持 GORM 事务/PreparedStmtTX 提取原 SQL 事务，普通 DB/未知 wrapper 拒绝。
- 相同身份与指纹追加幂等，冲突返回 `outbox.ErrConflict`，宿主须回滚。Appender 不能跨原事务复用。
- New 借用池，不执行 DDL。宿主显式迁移 [schema.sql](schema.sql)（Schema 嵌入同一源）；CREATE TABLE IF NOT EXISTS 不能升级已有表。
- Store 领取使用它自己的短调度事务与 FOR UPDATE SKIP LOCKED。这是业务提交后的阶段，token/version/state/lease 栅栏保护结算。

## 时间、计数与升级

标准调度 DATETIME(6) 保存 **UTC 时钟数字**。DATETIME 没有偏移，adapter 以 UTC 日期字符串写入、显式格式读取数据库 UTC 时钟，避免 driver loc 二次平移。业务/运维可显示 UTC+8，不能按连接 location 重新解释这些原数字，不能批量平移历史 DATETIME 或改写消息 occurred_at。

新 publishing 领取把 next_attempt_at 对齐 lease 到期，各活动状态按下一可处理时刻共同扫描。旧在途行下次领取时归一；升级公平扫描前，宿主评估或排空旧活动记录。消息字节不改写。

attempt_count 计领取（含租约恢复）；failure_count 只计成功栅栏 Retry/Quarantine（含内容损坏隔离），Confirm/reclaim 不增加。它不是模型或业务重试次数。v0.2.1 Store 要求 failure_count 和 updated_at；宿主做加法迁移并核对日志，不能从旧 attempt_count 推断失败。完整版本边界见[兼容与升级](../../docs/03-维护与验证/兼容与升级.md)。

## 显式原行重排

[Appender.RequeueConfirmed](requeue.go)是可选恢复原语：宿主先验证业务效果缺失与原消息、批准并持久记录请求，SDK 在同一宿主事务中条件改变**原 published 行**。record ID/version/指纹栅栏保持原字节和计数，保存宿主请求 ID 与新 version；不直接 publish，不授权模型或外部副作用重发。

它额外要求 schema 中的 manual_replay_request_id/manual_replay_version 列；一般 append/claim/confirm/retry 不需要这些可选列。transport_confirmed_at 保留 prior PUB 证据。未知返回后宿主查原治理账本，重复调用不静默成功。

## 验证入口

- [原事务](../../tests/integration/mysql_test.go)、[GORM](../../tests/integration/gorm_test.go)。
- [八组合时区](../../tests/integration/mysql_timezone_test.go)：driver UTC/UTC+8、SQL session +00:00/+08:00、参数插值 off/on。
- [计数schema升级](../../tests/integration/mysql_failure_count_upgrade_test.go)、[原行重排](../../tests/integration/mysql_requeue_test.go)。

隔离证明不覆盖任意历史宿主表，不授权生产迁移或重放。本包不是回执型 Outbox；后者见 [delivery/mysql](../../delivery/mysql/outbox.go)及其独立 [schema.sql](../../delivery/mysql/schema.sql)。
