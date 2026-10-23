# RocketMQ 事务协议 v1

本包与Python `reliable_messaging.transaction`提供纯协议契约，不执行网络、数据库、业务重试或迁移。原事务适配与发送器在独立包实现；固定预发布、真实门禁及宿主采用状态分别见[发布索引](../docs/releases/README.md)和[事务 SDK 接入](../docs/02-接入指南/RocketMQ事务SDK.md)。

- FrozenPlan冻结首次operation、owner/scope/幂等键、稳定业务input hash，以及每条分支的原Message、wire和固定路由。访问器不暴露可变正文。
- 宿主先查已持久化的幂等结果，再生成新ID、时间、密文。已有计划通过MatchRequest检查稳定业务输入并复用首次wire；并发首次插入失败者也读取同一个赢家。数据库唯一约束及并发隔离由[事务存储适配](../storage/transaction/README.md)实现与验证。
- 业务Message fingerprint与wire hash分别保存。路由修订会改变计划摘要，不改写原业务ID/fingerprint。
- Reference绑定owner、scope、operation/branch、原消息身份、固定路由、wire/plan/fingerprint、执行fence与writer epoch。Checker只确认完全相同的持久化凭证。
- 当前fence的execution和branch都为Committed才Commit，都为Aborted才Rollback；缺失、Prepared、矛盾、不同fence/owner等均Unknown。lease过期本身不证明中止。DB故障由Checker实现返回Unknown。
- 半消息、本地事务、Broker二次确认使用三个独立结果类型；本地Committed与二次确认Unknown可以同时成立。它们均不表示消费效果或业务回执完成。

计划摘要的域为`rm-tx-plan-draft-v1\0`，每个字段使用8字节大端长度前缀；分支顺序被冻结。Go/Python共同读取[协议向量](../contracts/transaction-plans-draft-v1.json)，包括多分支、非ASCII身份和二进制wire。

本批测试证明不可变数据、身份绑定及状态判定，不证明原事务原子性、执行器隔离、资源关闭、Broker恢复窗口或业务验收。正式 Sender 的存储、跨语言与故障证据由 infra M2 总表记录，不能用纯协议测试代替；宿主迁移属于 M3。物理路由由infra审核清单提供，本包不创建资源，也不将旧NORMAL Topic原地改为TRANSACTION。

同一代计划的重试保留首次wire，但宿主可在旧执行被证明确认Aborted后显式授权新fence。每个执行记录以operation/fence/epoch区分并保持终态单调；旧Aborted不能改回Prepared。根计划的最新执行状态不能替代旧半消息的精确凭证。宿主授权、claim/CAS和迟到提交隔离由原事务存储适配实现，Checker不创建新执行。

请求、计划代次、执行fence是三个不同身份。IAM等宿主若发现原计划依赖的业务版本已失效，可在证明旧计划从未提交且旧执行均已中止后，有界创建新的operation/计划代次；旧计划正文与终态不改写。并发首次请求仍选择同一个持久化赢家。MatchRequest只检查业务输入，不能代替宿主对当前活动计划、重建授权和版本CAS的检查。

## 回查确认观察

可选 `RecoveryReceiptRecorder.RecordRecoveryFinal`（Python 对应
`record_recovery_final`）只记录已收到成功 EndTransaction RPC 回复的精确分支。
Go/Python 运输适配在既有有界 worker 内调用它，数据库工作另用回查预算和新协议事务；
不借用业务事务，不取得或重建原 winner token，不改变 execution/witness 终态，
不运行 executor、重新发半消息或授权业务重试。Checker.Check 仍然只读。

MySQL、GORM 复用的 SQL Store、Go Mongo 及 Python SQLAlchemy Store 对精确
owner/store/Reference 与相符的 Committed/Aborted 证据再次校验；Prepared、Unknown、
错误 fence/wire 或矛盾结果均拒绝。重复同一正向观察幂等，原正向记录不能被 Unknown
降级。构造不执行 DDL，codec、schema 和原业务 wire 不变。

`CheckCommit/CheckRollback` 是决定累计数，`RecoveryCommitConfirmed` / Python
`recovery_commit_confirmed` 才是运输适配观察到的回查 RPC 确认累计数。
`RecoveryAuditRecorded/Errors/Unsupported`（Python 对应 snake_case）分别记录
持久化观察成功、失败和旧 Store 未实现可选能力；它们也不是当前 Broker 未决数量。
历史累计 FinalUnknown 不能直接当作当前未决 gauge 或最老未决年龄。

RPC ACK 与审计数据库提交之间仍存在崩溃/断连窗口；审计失败不会撤销 Broker 已接受的
确认。未证实记录代表确认观察缺口，不能断言 Broker 仍有半消息；报警、准入保护和
具名恢复必须保留该区别。Broker 有限回查结束后，不能依赖再次回查自动补齐缺失审计，
也不能仅凭消息被某一订阅消费就假定所有订阅完成。

## 持久化观察快照（M3 T24-C，尚未固定发布）

可选 `ObservationReader.ObserveProtocol` / Python `observe_protocol` 借用宿主的
primary pool，读取配置 owner 的协议元数据；查询不读取 plan、Reference、wire 或
业务正文，不改变业务/witness、安装 schema、补发消息或启动后台任务。Go 由 context
提供预算，无 deadline 时默认 2 秒；Python `budget_seconds` 默认 2 秒。数据库错误、
取消、错误 writer binding/schema 或未知状态使整个采样失败，不能作为零积压。
MySQL 使用单条非锁定语句并流式归并；Mongo 使用独立只读 snapshot session，
两次读取共享快照，不借用业务 session，不开启可提交的业务事务。

| 字段 | 含义 |
|---|---|
| reserved / preparing executions | Prepared 且尚未/已经开始准备半消息；每个 execution 只计一次 |
| prepared expiry age | 已开始准备的执行距数据库 lease 到期的时间，不是发送年龄或回滚证明 |
| confirmation gap branches | 本地终态、有 Prepared/Unknown 半消息审计，但缺少正向最终确认审计 |
| oldest confirmation observation age | 从 half audit（缺失时 final audit）到数据库当前时间的最大值；是观察到的暴露年龄下界，不能用最近 Unknown final 重置 |
| unaged confirmation gaps | 缺少审计时间的确认缺口；零年龄不能解释为刚发生 |
| untracked aborted branches | 已开始准备、已中止但无半消息结果且无正向最终确认；可能未发送，也可能在持久化前丢失确认，不计入已知确认缺口 |
| unobserved committed branches | 已提交但缺少 Prepared 半消息审计；与正常 Sender 的完整观察不同，需单独诊断 |
| metadata conflicts | execution/witness 终态或正向最终确认矛盾，或缺少分支元数据 |

快照不是 Broker 当前半消息数量，也不证明订阅或业务 receipt 完成。正向回查审计可
关闭对应缺口；没有再次回查的 ACK/审计丢失仍需具名处置。历史扫描在预算内流式完成，
超时丢弃部分结果；不承诺任意数据规模下都能采样。宿主指标、告警与准入保护仍需独立
接入验收，不能用本接口的存储测试代替。[共用语义向量](../contracts/transaction-observation-v1.json)
由 Go/Python 同时验证；真实存储仅在隔离 CI 验证。
