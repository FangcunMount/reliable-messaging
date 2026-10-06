# Go 兼容与升级策略（M6-01）

截至 2026-10-03（UTC+8），Go 最新预发布版本为 `v0.3.0-m6.4`。IAM 主线及最后一次实际二进制核验固定 `v0.3.0-m6.1`；本日12:42～12:43重新读取QS API、两Collection和三Worker的实际Go build info，均固定 `v0.3.0-m6.4`／Go1.25.12／`component-base v0.8.0`，无module replace。API源为309218630，Collection／Worker源为9128de4a。IAM主线仍c8fc1fe6；本次运维连接不能重新读取其Docker，不把主线未变当作生产容器未变。

以下固定Go线的支持基线、状态限制和升级规则。**兼容规则已经定义不等于Go v1.0已经发布或生产性能已验收**；M5业务时间／资源、M4压力回退及M6适用观察仍按各自证据关闭。Python已有独立预发布线，见[Python SDK](../../../python/README.md)，不计入本Go线的最低版本、API或稳定保证。

| 维度 | 当前基线 | 声明边界 |
| --- | --- | --- |
| Go | SDK最低1.25.9；建议及基础设施验证工具链1.25.12。IAM声明1.25.9、QS声明1.25.12。 | 最低SDK默认fmt／vet／test／race已在1.25.9与 `GOTOOLCHAIN=local` 下通过；完整双库／NSQ、Redis及宿主运行证据为1.25.12。其它Go补丁／平台不是本次已验证组合，不外推两宿主完整运行下限。 |
| MySQL | 支持基线下限8.0.36，已验证组合8.0.36与8.0.44。宿主最近实际数据库核验为8.0.36；当前SDK固定摘要CI实报8.0.44。 | 只声明这些具体组合；不是所有介于两者之间或更高补丁均已验证。低于8.0.36不在本线支持范围；原事务、唯一身份、租约／栅栏及显式加法迁移要求不变。 |
| MongoDB | 支持基线下限7.0.31，已验证组合7.0.31与7.0.37，必须使用支持事务的replica set。宿主最近实际核验为7.0.31；当前SDK固定摘要CI实报7.0.37。 | 单机非replica-set、低于7.0.31和其它补丁不在已声明组合内。事务活跃性、未知提交、领取及必需索引须按实际驱动和宿主迁移保留；不由此承诺任意历史业务文档或服务镜像兼容。 |
| NSQ | 唯一受支持Go broker基线为nsqd／nsqlookupd1.3.0；客户端 `go-nsq` 1.1.0。 | 不推定其它版本或未来多节点故障域。NSQ确认只表示传输接受，内存队列下可在强杀后丢失；必要效果仍由宿主事实／回执对账与安全恢复。RabbitMQ不在Go线支持范围。 |
| 驱动 | `go-sql-driver/mysql`1.9.3、`mongo-driver`1.17.6、`gorm.io/driver/mysql`1.6.0、GORM1.30.0、`go-nsq`1.1.0。 | 这些是固定依赖组合；替换、override或升级驱动须重验事务、确认、时间和恢复，不自动扩展支持范围。 |
| 可选Redis signaling | `go-redis/v9`9.16.0，当前真实Redis隔离CI用Redis7镜像。 | 仅最佳努力提示，不替代Outbox、接单回执或权限权威核验。Redis7镜像不是全部精确补丁支持证明；使用前由宿主核对实际版本及丢信号恢复，不把Redis可用性当作可靠消息前提。 |

当前主线c0efdf9的[Go检查](https://github.com/FangcunMount/reliable-messaging/actions/runs/37097070671)与[真实双库／NSQ](https://github.com/FangcunMount/reliable-messaging/actions/runs/37097070511)均实际成功；后者日志包含MySQL8.0.44及Mongo7.0.37，不再把先前8.0.36／7.0.31描述为当前CI实际运行版本。隔离资源为固定摘要镜像，不能用一个可变tag推断所有补丁。

## Go API、wire及持久状态

| 合同 | 本线规则 |
| --- | --- |
| Go公开入口 | `message`、`outbox`、`relay`、`transport`／`transport/nsq`、`storage/mysql`／`storage/mongo`、`catalog`、`wire/legacy`／`wire/domain`及 `signaling`／`signaling/redis`；原事务、生命周期和资源归属均显式。宿主业务事件、路由、权限、幂等及重试授权不搬入SDK。 |
| 消息身份和指纹 | 身份为producer＋应用message_id＋逻辑destination；物理NSQ ID、attempt和claim不改业务身份。现行九字段长度前缀SHA-256的 `rm-fingerprint-draft-v1` 前缀保留，不能因发布v1.0而改名或重算历史指纹。payload和occurred_at原字节不重编码，未知扩展字段保留。业务schema_version由宿主提供，不把SDK版本当业务schema。 |
| 确认及结算 | 发布结果 `Confirmed`／`Unknown`／`Rejected` 保留原含义；Unknown可能已送达，SDK不据此新增业务执行。独立消费者分别负责效果／回执；失败中转须持久确认后才FIN原消息。 |
| MySQL标准表 | 宿主原事务内追加；基础新版必须有failure_count／updated_at和必需唯一／到期索引。可选RequeueConfirmed另需manual_replay_request_id／manual_replay_version及宿主原事务内审批审计。SDK不自动DDL或创建自己的数据库。 |
| Mongo标准集合 | 宿主原SessionContext及活动事务；使用标准身份、不可变内容和显式索引。created_at／updated_at、缺failure_count的兼容读取与宿主治理可读性分别核对；读到旧文档不等于允许重放未知业务。 |
| 状态交接 | pending／publishing／retry_wait／quarantined及published逐项归属；确认、租约、claim token和version栅栏不得绕开。published是传输事实，不能用它删除尚未对账的业务责任。 |
| 时间 | 标准MySQL调度DATETIME(6)采用显式UTC数字，Mongo保留绝对时刻；界面和运维统一UTC+8。升级不依loc转换已存调度或重写消息occurred_at；宿主业务时间仍由宿主合同决定。 |

宿主负责业务事务、本地 Outbox 表／集合和迁移、Relay 生命周期、消费幂等及业务重试授权。SDK 在技术重试中保留原应用消息 ID 与不可变 wire 字节。发布确认丢失须记为结果未知；重复物理投递不得新增业务效果。更换 broker 或看到 Outbox `published` 都不能自动填平事务提交到消费者效果之间的缺口。

## 升级与降级

1. 两宿主固定 SDK 发布标签。升级前比较公开 Go API、wire fixture、消息身份、必需列与索引、状态转换和驱动版本；运行 SDK 集成验证及受影响的 IAM、qs-server 事务、Relay、消费与回退路径。
2. 先由宿主完成加法 schema／索引变更，再启动新写入者；回退窗口内旧执行者仍须能读取当前结构。逐状态指定 `pending`、`publishing`、`retry_wait`、`quarantined` 和失败中转消息的责任执行者，交接前停止竞争写入者。
3. `v0.2.1`→`v0.3.0-m6.1` 不改变 SDK MySQL／Mongo Store 或 Relay schema，但新增目录、wire、Subscriber、Provisioner 和托管 Publisher API；仍须核对宿主数据与消费者。新失败预算记录**不支持直接交给 `v0.1.0` 降级处理**，因为旧版不维护新增的失败元数据。
4. `v0.3.0-m6.1`→`v0.3.0-m6.2` 新增 MySQL `Appender.RequeueConfirmed` 和参考建表 DDL 的 `manual_replay_request_id`、`manual_replay_version` 两列；现有发布／领取／Relay 主路径没有改动。此方法**只重排已经确认的原行**，调用前由宿主核验缺失的业务效果、持久审批及原身份，并在同一事务中写入审批记录。SDK 不自动迁移宿主数据库；使用前须核对加法列已安装（qs-server 的标准表迁移 84 已包含），否则让数据库报错，不省略审计。两个已发布标签的独立进程已在一次性 MySQL 8.0.36／NSQ 1.3.0 上验证单条顺序交接：`m6.2` 重排原行后，`m6.1` Relay 可按原身份和正文再次投递，审计关联仍保留，尝试数从 1 增为 2；两份物理投递仍须消费者幂等。此证据不覆盖真实 IAM／QS 镜像回退、并行执行者、全部状态或生产数据水位。旧版执行者对新增恢复状态尚无完整宿主降级演练，恢复请求或待 Relay 行未结案时，不得仅回滚 SDK／镜像就把责任交给旧版。
5. 宿主镜像回退后重新核对真实业务效果与授权新鲜度。镜像可启动、当前队列为空或一份测评成功，均不能代替逐流恢复证明。
6. `m6.2`→`m6.3`新增通用状态构造器，`m6.3`→`m6.4`新增signaling／Redis adapter；对应跨度既有message、双Store、Relay和NSQ运行对象不变。新增接口不是宿主业务迁移许可。现役QS309修复首次Run Claim死锁；旧9128仅保留原静止／冻结写者窗口恢复条件，**不能作为持续新接单压力下的合格回退**。宿主当前已接单Run／v2结果格式和冻结配置的回退资格仍按宿主对应证据核对，不由SDK支持版本表代替。

v1.0前公开API仍属预发布范围；变更必须说明迁移办法并验证IAM／qs-server调用者。v1.0发布后，在整个v1期间保留已承诺的公开API、wire身份与持久状态兼容；修补问题不改变上述业务语义。弃用入口必须提供替代及迁移说明、核对所有受支持调用者和仍有效回退窗口；只标弃用，不在v1兼容版本中删除。必需字段的破坏性修改或API删除须进入v2，并保留可执行的状态／数据迁移说明。此规则不承诺无限期保留无人使用的component-base旧入口；其通用消息职责已由v0.8.0实际退役。

Python已有独立 `python/v0.1.0a1` 预发布，但不列入Go稳定支持矩阵；不由Go CI代验Python／qs-ai业务恢复。RabbitMQ仍为独立评估草稿，不属于本线。后续扩展版本、驱动、broker或拓扑，先补实际适用验证再修改支持矩阵；不得自动外推或跳过宿主故障、容量与业务验收。
