# 原业务事务 witness 适配（provisional）

Go `mysql`（database/sql、GORM）和 `mongo`、Python `reliable_messaging.transaction_mysql` 保存独立事务消息协议记录，不复用发布 Outbox。接口在完整双存储和正式 Sender 故障验收前不作稳定承诺。测试证据与发布状态见 infra 的 M2 执行总表。

宿主显式安装 MySQL schema / Mongo `InstallSchema`，插入 owner、store_id、writer_epoch、schema_version 绑定；schema 安装、epoch 修改与生产迁移不发生在构造或启动里。MySQL 两语言模板必须保持相同字节。Mongo 使用单 execution 文档内的分支 witness，和业务文档一起处于原多文档事务；无 TTL 索引或自动删除。

`Reserve` 持久化完整冻结计划、Prepared execution 和分支 witness，并仅给赢家返回执行 capability。`Lookup` 和并发失败者的 token 全零，只能观察；不能 Bind、Abort 或发送。`BeginPrepare` 在任何 Broker I/O 前一次性消费赢家资格；重启、超时或重复 Execute 不自动再发一套半消息。若准备后进程消失，由有界中止/回查收敛，宿主确认旧执行 Aborted 后才显式 Retry / Replan。

宿主在半消息全部确认后开启业务根事务，以 Bind 作为事务内第一项协议操作，然后写业务事实、调用 CommitWitness，最后亲自提交根事务。任何 guard / witness 错误必须回滚整个根事务，不得捕获后继续提交。CommitWitness 成功只表示写入原事务，不表示已持久提交；savepoint release 同样不能证明根提交。SQLAlchemy 仅支持直接 Engine 绑定的 AsyncSession 或宿主直接持有的 AsyncConnection；拒绝嵌套绑定、外部事务 join、事务替换和物理 AUTOCOMMIT。

MySQL guard 的条件写入持有同一 execution 行的锁；Mongo guard 对同一 execution 文档条件写入并参与写冲突。中止器必须在这条记录上竞争，不能凭 lease 到期直接返回 Rollback。新 fence / operation 保存独立历史，不修改旧终态。Mongo callback 可能重入，每次绑定当前 session，复用同一冻结内容；网络发消息、UUID/时间生成、加密和外部调用放在 callback 外。

Checker 使用宿主提供的权威主库/primary-only资源和新读取边界，按精确 Reference 查询。缺失、损坏、Prepared、锁等待、数据库错误和不确定提交均保持 Unknown。它不执行业务、不授权重试、不新发消息。Store 自己的短协议事务不代替业务事务，关闭时不释放宿主连接池/Client。

隔离验证入口为 `make transaction-storage-integration`：固定镜像、随机 Compose 项目、CPU/内存上限、独立 tmpfs 和数据库，缺依赖失败，退出只清理本次项目。Python required CI 同时运行源码和隔离安装 wheel。Driver callback 重入/commit retry 的测试与真实网络层提交丢回复属于不同证据，后者由后续 M2 故障矩阵验证。
