# Go 接入

本文面向把 SDK 嵌入现有 Go 服务的维护者，描述当前源码的标准租约 Outbox、NSQ 发布与订阅接线。安装版本及候选范围见[发布索引](../releases/README.md)；回执型 Outbox 是独立合同，不能套用下面的标准 Store 结算。

接入的关键是把“应发布的原消息”写入宿主正在使用的业务事务，再由宿主启动 Relay。SDK 不建立业务数据库、不隐式建表、不替宿主提交事务，也不决定业务是否允许重新执行。完整设计见[事务与 Outbox](../01-核心设计/事务与Outbox.md)和[生命周期与恢复](../01-核心设计/生命周期与恢复.md)。

## 接入前选择

| 宿主已有能力 | 接入方式 | 必须由宿主完成 |
|---|---|---|
| `database/sql` MySQL 事务 | `storage/mysql.Bind(*sql.Tx)` | 原事务的开始、提交、回滚；显式迁移标准表 |
| GORM MySQL 事务 | `storage/mysql.BindGORM(*gorm.DB)` | 传入 Transaction callback 的原 `tx`；保留事务所有权 |
| Mongo `WithTransaction` | `storage/mongo.Bind(SessionContext, Collection)` | 同一 client 的 collection；事务支持与索引；callback 可重复执行 |
| 已提交的标准 Outbox | Store + `relay.New` + `Relay.Run` | 重试策略、观测、运行监督、停止预算 |
| 现有 NSQ Producer | `nsq.New` | Producer 的驱动超时与最终 Stop |
| SDK 自持有 NSQ Producer | `nsq.NewManagedPublisher` | 明确允许连接/Ping；显式 Close，超时后确认实际排空 |
| 业务消息消费 | `nsq.NewSubscriber` + `Subscribe` | 业务幂等、持久失败回调、节点/channel 准备 |

依赖版本由 [`go.mod`](../../go.mod) 给出。公开接口参考源码及 Go 包文档；以下代码是宿主函数片段，不是自动运行的迁移或发布工具。参数中的业务写入函数由宿主提供。

## 1. 先固定消息身份和字节

使用 [`message.New`](../../message/message.go) 创建不可变消息，明确 `Producer`、`ID`、逻辑 `Destination`、事件与 schema 版本、作用域、内容类型、原发生时间和原 payload。同一意图重用第一次持久化的身份与字节；不在事务重入或技术重投时重新生成时间、ID、JSON 或密文。

`Message` 的 payload 不会自动变成 NSQ envelope。`nsq.Publisher.Publish` 发的是已保存的 payload 字节，逻辑路由由宿主映射为 topic；如果消费者需要应用 ID、metadata、domain envelope 或 protected wire，宿主必须在第一次持久化前编码，或接入保留原消息身份的专用 Publisher。不得把 NSQ 物理 ID 当作原应用 ID。参见[消息身份与协议](../01-核心设计/消息身份与协议.md)。

## 2. 在原 MySQL 事务中追加

下面函数属于宿主：它创建一笔业务事务，在同一笔事务里执行业务写入和 Outbox 追加，然后提交。SDK 的 `Bind` 和 `Append` 不开启另一笔事务。任何一步失败返回错误，宿主负责回滚。

```go
package integration

import (
    "context"
    "database/sql"
    "time"

    "github.com/FangcunMount/reliable-messaging/message"
    sdkmysql "github.com/FangcunMount/reliable-messaging/storage/mysql"
)

func WriteSQL(ctx context.Context, db *sql.DB, m message.Message, due time.Time,
    writeBusiness func(context.Context, *sql.Tx) error) error {
    tx, err := db.BeginTx(ctx, nil)
    if err != nil { return err }
    defer tx.Rollback()
    if err = writeBusiness(ctx, tx); err != nil { return err }
    appender, err := sdkmysql.Bind(tx)
    if err != nil { return err }
    if err = appender.Append(ctx, m, due); err != nil { return err }
    return tx.Commit()
}
```

GORM 的入口只接受能解析为原 `*sql.Tx` 的受支持事务连接，普通 `*gorm.DB` 或未知包装不被接受。已结束的事务在 Append 时失败，不应保存 Appender 跨请求复用。

```go
package integration

import (
    "context"
    "time"

    "github.com/FangcunMount/reliable-messaging/message"
    sdkmysql "github.com/FangcunMount/reliable-messaging/storage/mysql"
    "gorm.io/gorm"
)

func WriteGORM(ctx context.Context, db *gorm.DB, m message.Message, due time.Time,
    writeBusiness func(*gorm.DB) error) error {
    return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
        if err := writeBusiness(tx); err != nil { return err }
        appender, err := sdkmysql.BindGORM(tx)
        if err != nil { return err }
        return appender.Append(ctx, m, due)
    })
}
```

相同身份与指纹重复追加是幂等；同一身份的内容变化返回 `outbox.ErrConflict`。宿主不能吞掉冲突后继续提交业务事实。标准表安装和升级见 [MySQL 包入口](../../storage/mysql/README.md)；调度列采用 UTC 时钟数字，UTC+8 显示不授权改写历史数据。

## 3. Mongo callback 内重新绑定

在 `WithTransaction` 之前准备消息及 due；每次 callback 调用都使用它的 `SessionContext` 重新绑定。业务操作和 Outbox collection 必须来自同一 client，且所有事务操作传入同一个 callback context。不要在 callback 内发 MQ、调用模型或制造新的业务身份。

```go
package integration

import (
    "context"
    "time"

    "github.com/FangcunMount/reliable-messaging/message"
    sdkmongo "github.com/FangcunMount/reliable-messaging/storage/mongo"
    driver "go.mongodb.org/mongo-driver/mongo"
)

func WriteMongo(ctx context.Context, session driver.Session, collection *driver.Collection,
    m message.Message, due time.Time, writeBusiness func(driver.SessionContext) error) error {
    _, err := session.WithTransaction(ctx, func(sc driver.SessionContext) (interface{}, error) {
        if err := writeBusiness(sc); err != nil { return nil, err }
        appender, err := sdkmongo.Bind(sc, collection)
        if err != nil { return nil, err }
        return nil, appender.Append(m, due)
    })
    return err
}
```

只有 SessionContext 不够：Bind/Append 还检查原 session 的事务正在运行。该检查依赖固定 Mongo driver v1 的 `XSession`，升级驱动必须重新验证。宿主在事务外显式安装 `Indexes()`，核对 replica set、持久性、查询计划和生产负载。见 [Mongo 包入口](../../storage/mongo/README.md)。

## 4. 由宿主运行和监督 Relay

Store 借用宿主连接池；`relay.New` 只验证配置，`Run` 才扫描并发送已提交意图。下面数值仅用于说明配置关系，不是生产业务目标。租约必须大于发布与回写预算之和；Observe 必须快速、不阻塞且并发安全，Retry 必须由宿主给出。

```go
package integration

import (
    "context"
    "database/sql"
    "time"

    "github.com/FangcunMount/reliable-messaging/relay"
    sdkmysql "github.com/FangcunMount/reliable-messaging/storage/mysql"
    "github.com/FangcunMount/reliable-messaging/transport"
)

func RunMySQLRelay(ctx context.Context, db *sql.DB, publisher transport.Publisher,
    retry relay.RetryPolicy, observe relay.Observer) error {
    store, err := sdkmysql.New(db)
    if err != nil { return err }
    runner, err := relay.New(store, publisher, relay.Config{
        Concurrency: 4, PollInterval: time.Second, Lease: 30 * time.Second,
        PublishTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
        Retry: retry, Observe: observe,
    })
    if err != nil { return err }
    return runner.Run(ctx)
}
```

宿主记录 Run 返回的扫描失败并决定是否监督重启；同一 Relay 的 Run 不允许重叠。提交后的 Wake 只是降低扫描延迟，丢失 Wake 或重启仍须扫描持久状态。`published` 仅说明传输确认，不能证明业务已接单或执行完成。

停止时先停止新业务接纳和订阅入口，再取消 Relay 并等待 Run 返回；随后 Drain 借用型 Publisher 或 Close 自持有型 Publisher，最后关闭其所需的宿主资源。NSQ 驱动调用没有 context 参数，调用者超时不能证明底层发送结束。具体 Stop/Close 超时处理见 [NSQ 包入口](../../transport/nsq/README.md)。

## 5. 消费者持久提交之后才结算

`NewSubscriber` 不启动网络；`Subscribe` 显式连接失败 channel，再连接业务 channel。宿主选择直接 nsqd 或 lookupd 发现、物理重投预算和并发限制；业务 channel 与失败 channel 必须在允许首次发布前准备好。多实例广播与工作分摊的 channel 选择由宿主业务决定。

`transport.Handler` 返回 nil 且未显式结算会 Ack；返回错误且未结算会 Nack。已显式 Ack/Nack 的结果优先，不能依靠随后返回错误撤销 Ack。下面函数把持久幂等处理留给宿主，只有提交成功才返回 nil：

```go
package integration

import (
    "context"

    "github.com/FangcunMount/reliable-messaging/transport"
)

func DurableHandler(commit func(context.Context, transport.Received) error) transport.Handler {
    return func(ctx context.Context, delivery transport.Delivery) error {
        return commit(ctx, delivery.Message())
    }
}
```

`commit` 必须在原应用身份上做业务幂等、持久接单或明确持久隔离，不能只是把任务放进进程内队列。订阅失败回调同样须先持久保存 `legacy.FailedHandoff` 再返回 nil。预算耗尽后跳过业务 handler，原消息仅在失败中转被确认后 FIN；丢确认可能形成重复失败记录，宿主失败审计也需去重。

## 核验入口

- [GORM 原事务集成](../../tests/integration/gorm_test.go)、[MySQL 原事务与状态](../../tests/integration/mysql_test.go)。
- [Mongo rollback/callback 重入](../../tests/integration/mongo_test.go)。
- [NSQ 订阅合同](../../transport/nsq/subscription_test.go)、[实际订阅集成](../../tests/integration/nsq_subscription_test.go)、[失败审计集成](../../tests/integration/nsq_failure_audit_test.go)。
- [宿主生命周期示例](../../examples/host-lifecycle/main.go)与[测试与故障验证](../03-维护与验证/测试与故障验证.md)。

隔离测试、版本发布和宿主生产验收分别记录。完成上述接线后，还需沿宿主原业务事务、幂等、失败恢复和实际停止路径核对；具体责任见[宿主接入边界](宿主接入边界.md)。
