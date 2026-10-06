# 可执行宿主接入参考

这里的程序调用公开 SDK API。它们使用合成消息；领域事件、重试授权和生产配置由宿主另行提供。接入说明见[Go 接入](../docs/02-接入指南/Go接入.md)，原 M1 草案说明保留在[历史材料](../docs/_archive/milestones/examples-m1-readme.md)。

| 示例 | 实际证明 | 不证明 |
|---|---|---|
| [transactional-publisher](transactional-publisher/main.go) | 宿主原 `sql.Tx` 调用 `storage/mysql.Bind`，使用公开 `Schema` 与 `message.New`；真实 MySQL 上业务和标准意图共同提交/回滚，nil 事务被拒绝 | Broker 投递、消费幂等、生产迁移或性能 |
| [host-lifecycle](host-lifecycle/main.go) | 公开 Relay 显式运行；宿主取消接纳后等待已领取投递写回，再关闭资源 | 测试替身 Store 的持久化或数据库租约栅栏；这些由隔离 Store 测试验证 |

`make integration` 在专用 Docker MySQL 内创建 `rm_example_test`，构建并运行事务示例，再运行生命周期示例。事务示例只允许 loopback TCP 与该前缀数据库，宿主显式做 DDL 和连接关闭，不会向 MQ 发送。

生命周期示例也可单独运行：

```sh
go run ./examples/host-lifecycle
```

`make check` 构建/检查这两个包；真实数据库执行由已有[隔离作业](../.github/workflows/integration.yml)和[脚本](../scripts/integration.sh)承担。不要把编译成功解释成原事务已在数据库验证。
