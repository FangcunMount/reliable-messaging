# Go 接入

Go 接入首先要把一条业务写入和它的发布意图放进同一个原事务；然后装配独立扫描、真实 NSQ 发布与持久消费。SDK 不替宿主建业务数据库、迁移表、开始／提交业务事务，也不提供通用业务 Inbox。

本篇从一个 `example.accepted` 事件给出连续路径：准备原 wire → 业务与 Outbox 提交 → Relay 发布 → 消费保存一次业务效果 → 停机排空。示例是单个显式 nsqd、合成数据和标准 Outbox；回执型业务不能直接套用它的 published 结束点。

## 先知道已有示例验证到哪里

两个可运行参考承担不同作用：

| 入口 | 实际行为 | 没有验证的部分 |
|---|---|---|
| [transactional-publisher](../../examples/transactional-publisher/main.go) | 一次性 MySQL 中写 example_business 和 rm_outbox，检查共同提交／回滚 | 不发送 NSQ，不验证消费者 |
| [host-lifecycle](../../examples/host-lifecycle/main.go) | 显式启动 Relay、取消接纳、等待已接纳结算 | Store/Publisher 是替身，不证明数据库或 Broker 持久性 |

`make check` 验证默认 Go 代码与文档；`make integration` 才使用隔离数据库／Broker 执行相应参考和故障用例。不要把上述两个参考拼成“已经完成宿主生产验证”。

安装时固定经过宿主验证的 SDK tag，不用浮动分支替代版本合同。当前制品及候选范围见[发布索引](../releases/README.md)，最低 Go 与驱动约束见 [go.mod](../../go.mod)。

## 1. 宿主先安装表，明确连接和路由

标准 MySQL Outbox 用宿主业务数据库中的 [schema.sql](../../storage/mysql/schema.sql)。由宿主迁移体系执行 DDL；CREATE TABLE IF NOT EXISTS 不能升级已有表。Store.New 和 Bind 不迁移 schema。

本文的合成业务表：

```sql
CREATE TABLE example_business (
  id VARBINARY(64) PRIMARY KEY
) ENGINE=InnoDB;

-- 消费端示例将效果本身作为持久幂等事实，仅处理本文固定 producer。
CREATE TABLE example_effects (
  event_id VARBINARY(128) PRIMARY KEY,
  body_hash BINARY(32) NOT NULL,
  body BLOB NOT NULL
) ENGINE=InnoDB;
```

生产业务需换成自己的权威事实和唯一约束，多 producer 的去重键还应包含约定的生产者／作用域。示例用二进制键区分 Event-A 和 event-a，避免数据库默认 collation 合并不同应用身份。不要为了接 SDK 强行让所有消费新增同一种 Inbox。

宿主创建并保持 MySQL pool、NSQ HTTP/TCP 配置和停止预算。调度列保存 UTC 时钟数字，界面可显示 UTC+8；不需要 SDK 改宿主 session 时区，更不能批量平移原历史数字。

## 2. 第一次准备消息时就固定原 ID 和 wire

完整 Revision2 wire 放入 Message.Payload，基本 NSQ Publisher 后续直接发送它。Message 不会自动编码成网络 envelope：

```go
package integration

import (
    "context"
    "database/sql"
    "time"

    "github.com/FangcunMount/reliable-messaging/message"
    sdkmysql "github.com/FangcunMount/reliable-messaging/storage/mysql"
    "github.com/FangcunMount/reliable-messaging/wire/legacy"
)

func Prepare(id string, occurredAt time.Time) (message.Message, error) {
    body := []byte(`{"event":"accepted"}`)
    wire, err := legacy.Encode(legacy.Envelope{
        UUID: id,
        Metadata: map[string]string{"event_type": "example.accepted"},
        Payload: body,
    }, legacy.Revision2)
    if err != nil { return message.Message{}, err }
    return message.New(message.Input{
        Producer: "example", ID: id, Destination: "example.events",
        EventType: "example.accepted", SchemaVersion: "1", Scope: "synthetic",
        ContentType: "application/json",
        OccurredAt: occurredAt.Format(time.RFC3339Nano), Payload: wire,
    })
}

func WriteIntent(ctx context.Context, db *sql.DB, id string,
    original message.Message, due time.Time) error {
    tx, err := db.BeginTx(ctx, nil)
    if err != nil { return err }
    defer tx.Rollback()
    if _, err = tx.ExecContext(ctx,
        "INSERT INTO example_business(id) VALUES (?)", id); err != nil { return err }
    appendTo, err := sdkmysql.Bind(tx)
    if err != nil { return err }
    if err = appendTo.Append(ctx, original, due); err != nil { return err }
    return tx.Commit()
}
```

Prepare 在可能重入的事务／重试之外执行，due 同样固定。WriteIntent 中没有 MQ 调用。任何业务或 Append 错误都阻止提交；相同消息身份内容改变返回 ErrConflict，不能吞掉它后提交业务。

Commit 返回异常时，不能仅凭错误判断业务未提交。应回查原业务身份和原 Outbox 决定结果；不生成新 ID 再“补一次”。实际业务幂等还需处理业务行重复，本例的普通 INSERT 仅用于展示原事务关系。

IAM 采用另一种合法适配：存原版本正文，Publisher 在发送副本上编码 Revision1。不要把本文“先存完整 wire”当作所有现有宿主已采用的方式；两者的原字节合同见[消息身份与协议](../01-核心设计/消息身份与协议.md)。

## 3. 第一条 PUB 前准备持久 channel

宿主用显式 HTTP 地址准备资源。以下为装配片段，所有地址均是隔离示例：

```go
provisioner, err := nsq.NewProvisioner(
    &http.Client{Timeout: 5 * time.Second},
    []string{"http://127.0.0.1:4151"},
)
if err != nil { return err }
if err = provisioner.EnsureChannel(ctx, "example.events", "example.worker"); err != nil {
    return err
}
failureTopic := legacy.FailedHandoffTopic("example.events", "example.worker")
if err = provisioner.EnsureChannel(ctx, failureTopic, legacy.FailedHandoffChannel); err != nil {
    return err
}
```

这一步覆盖每个可以接收 PUB 的 nsqd，而不只覆盖首个消费者连接的节点。部分准备失败就保持业务入口未就绪；新发布节点加入后重核范围。Provisioner 不推导端口、不准备临时 channel，也不会为宿主选择业务 topic/channel。

## 4. 装配 Publisher、Subscriber 和 Relay

先用有限 driver 超时构造 owned Publisher，再注册消费者，最后启动 Relay 和业务入口。装配失败也必须关闭已经获得的资源。

```go
driverConfig := driver.NewConfig() // driver 为 github.com/nsqio/go-nsq
driverConfig.DialTimeout = 2 * time.Second
driverConfig.ReadTimeout = 5 * time.Second
driverConfig.WriteTimeout = 5 * time.Second
driverConfig.HeartbeatInterval = time.Second // 必须小于 ReadTimeout。

publisher, err := nsq.NewManagedPublisher(nsq.ManagedPublisherConfig{
    Address: "127.0.0.1:4150", Driver: driverConfig,
    Routes: map[string]string{"example.events": "example.events"},
    MaxInFlight: 4,
})
if err != nil { return err } // 返回前实际连接并 Ping。

subscriber, err := nsq.NewSubscriber(nsq.SubscriberConfig{
    NSQDAddresses: []string{"127.0.0.1:4150"},
    Driver: driverConfig, DeliveryContext: serviceCtx,
    MaxInFlight: 4, MaxAttempts: 3,
    Retry: nsq.Backoff{BaseDelay: time.Second, MaxDelay: 30 * time.Second},
})
// 检查 err；失败时沿宿主统一清理路径关闭 publisher。
```

业务 handler 和失败 handler 的持久合同在下一节。Subscribe 显式启动网络，先连失败 consumer 再连业务 consumer；它的注册 ctx 只控制注册过程，DeliveryContext 控制后续投递。

```go
if err := subscriber.Subscribe(startupCtx, "example.events", "example.worker",
    handleAccepted, persistFailure); err != nil {
    // 保留 subscriber 实例并统一清理；不能丢掉部分注册资源。
    return err
}
store, err := sdkmysql.New(db)
if err != nil { return err }
runner, err := relay.New(store, publisher, relay.Config{
    Concurrency: 4, PollInterval: time.Second, Lease: 30 * time.Second,
    PublishTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
    Retry: func(c outbox.Claim, result transport.Outcome) relay.RetryDecision {
        if result == transport.Rejected || c.FailureCount >= 5 {
            return relay.RetryDecision{Quarantine: true}
        }
        return relay.RetryDecision{Delay: 5 * time.Second}
    },
    Observe: observe,
})
if err != nil { return err }
done := make(chan error, 1)
go func() { done <- runner.Run(relayCtx) }()
```

这些数值只是说明合法配置关系，不是生产性能目标。Observe 由宿主提供，必须快速、并发安全且不阻塞。此示例对 Unknown 延迟，对本地 Rejected 或预算耗尽隔离；它不授权未知模型调用或通知再次执行。

Run 扫描失败会返回，宿主必须读取 done、维护健康状态并决定退避重启。同一 Relay 不能重叠 Run。Wake 可以缩短提交后延迟，但周期扫描不能因有 Wake 而删除。

## 5. 把消费效果保存后再返回成功

下面函数展示持久效果幂等，表采用上文 example_effects：同一原 ID 重复正文不会创建第二条效果，正文冲突阻止成功。它仅处理固定 producer 的合成事件；多租户、业务准入和具体字段校验应在宿主协议层实现。

```go
// 片段需要 context、database/sql、bytes、crypto/sha256、errors 及 transport。
func SaveAccepted(ctx context.Context, db *sql.DB, m transport.Received) error {
    if m.ID == "" || m.Metadata["event_type"] != "example.accepted" {
        return errors.New("invalid example event")
    }
    digest := sha256.Sum256(m.Payload)
    tx, err := db.BeginTx(ctx, nil)
    if err != nil { return err }
    defer tx.Rollback()
    _, err = tx.ExecContext(ctx, `INSERT INTO example_effects(event_id,body_hash,body)
        VALUES (?,?,?) ON DUPLICATE KEY UPDATE event_id=event_id`,
        m.ID, digest[:], m.Payload)
    if err != nil { return err }
    var originalHash []byte
    if err = tx.QueryRowContext(ctx,
        "SELECT body_hash FROM example_effects WHERE event_id=?", m.ID).
        Scan(&originalHash); err != nil { return err }
    if !bytes.Equal(originalHash, digest[:]) {
        return errors.New("example event identity conflict")
    }
    return tx.Commit()
}
```

注册 handler 可直接返回 SaveAccepted 的结果。提交前不能 Ack；返回 nil 会 FIN，返回错误会进入 REQ／失败转交。已显式 Ack 后返回错误不能撤销 FIN。

实际业务如果需要“处理标记＋另一张业务表更新”，二者必须在同一个消费事务提交。本例将效果行自身作为权威事实；它不提供一次外部微信发送或模型调用的幂等保证。

persistFailure 同样必须在宿主失败表幂等保存 FailedHandoff 后才返回 nil，不能仅写日志或进程内 channel。原消息在失败中转 PUB 确认后 FIN，失败消息在失败账本提交后 FIN，两者之间的 Broker 窗口见[投递与消费](../01-核心设计/投递与消费.md)。

## 6. GORM 和 Mongo 怎样替换原事务绑定

GORM 传入 Transaction callback 的原 tx：

```go
return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
    if err := tx.Create(&businessRow).Error; err != nil { return err }
    appendTo, err := sdkmysql.BindGORM(tx)
    if err != nil { return err }
    return appendTo.Append(ctx, originalMessage, originalDue)
})
```

BindGORM 只识别支持的原 *sql.Tx／PreparedStmtTX，普通 DB 和未知包装拒绝。它不会追踪每个 GORM savepoint 的完整业务作用域；业务与意图必须遵守宿主一致的回滚边界。

Mongo 使用同一 client 的业务和 Outbox collection，提前在事务外安装 [Indexes](../../storage/mongo/store.go)，并确保部署支持事务。callback 可能重入：

```go
_, err := session.WithTransaction(ctx, func(sc driver.SessionContext) (interface{}, error) {
    if _, err := businessCollection.InsertOne(sc, businessDoc); err != nil {
        return nil, err
    }
    appendTo, err := sdkmongo.Bind(sc, outboxCollection)
    if err != nil { return nil, err }
    return nil, appendTo.Append(originalMessage, originalDue)
})
```

这里 driver 为 Mongo driver，不能与 NSQ driver 混用导入别名。每次 callback 重新 Bind，传同一个 sc；原消息、ID 和 wire 在 callback 外固定。不要在 callback 发 MQ 或调用模型。当前原事务活动检查依赖固定 Mongo driver v1 的 XSession，升级时重新验证，而不是认为换版本仍当然有效。

## 7. 按真实依赖退出

宿主先停止业务入口和订阅新接纳，保持在途 Handler 需要的 DB／发送依赖；等待 Subscriber 退出，取消 Relay 并读取 Run 结果，再 Close ManagedPublisher，最后关池。

Close 超时不能当作成功。ManagedPublisher 需要保留实例继续 Close；强制 Interrupt 后也要再次 Close 核验。借用型 nsq.New 则 Drain 后由宿主 Stop 原 Producer。Run 返回不证明真实 driver PUB 已结束。

装配片段中的早期 return 必须接入宿主统一清理路径；它们不是带完整资源管理的独立 main 文件。完整退出机制见[生命周期与恢复](../01-核心设计/生命周期与恢复.md)。

## 接好以后怎样核验

| 宿主应证明的事实 | SDK 可复用的验证入口 |
|---|---|
| 业务提交／回滚与意图一致 | [MySQL](../../tests/integration/mysql_test.go)、[GORM](../../tests/integration/gorm_test.go) |
| 重复身份幂等、不同正文冲突 | [MySQL 原事务](../../tests/integration/mysql_test.go)、[Mongo](../../tests/integration/mongo_test.go) |
| 提交后扫描、两个发送者栅栏 | [迟到 Relay](../../tests/integration/late_relay_test.go) |
| 原应用 ID 穿过 PUB，确认丢失不改 wire | [NSQ 发布](../../tests/integration/nsq_test.go) |
| Handler 提交前不 FIN、失败审计可恢复 | [订阅](../../tests/integration/nsq_subscription_test.go)、[失败账本](../../tests/integration/nsq_failure_audit_test.go) |
| 停机后没有未结束的 owned 发送 | [ManagedPublisher 实集成](../../transport/nsq/managed_publisher_integration_test.go) |

这些测试验证 SDK 局部机制。宿主还需要沿自己的事务、唯一业务键、失败账本、停止路径和真实部署逐项核对；当前主分支源码不自动等于已发布制品或生产行为。
