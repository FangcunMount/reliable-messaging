# RocketMQ 事务协议草案

本包与Python `reliable_messaging.transaction`提供纯协议契约，不执行网络、数据库、业务重试或迁移。接口在原MySQL/Mongo事务绑定证明之前保持provisional，不作为已发布事务消息能力。

- FrozenPlan冻结首次operation、owner/scope/幂等键、稳定业务input hash，以及每条分支的原Message、wire和固定路由。访问器不暴露可变正文。
- 宿主先查已持久化的幂等结果，再生成新ID、时间、密文。已有计划通过MatchRequest检查稳定业务输入并复用首次wire；并发首次插入失败者也读取同一个赢家。数据库唯一约束及并发证明属于后续存储适配。
- 业务Message fingerprint与wire hash分别保存。路由修订会改变计划摘要，不改写原业务ID/fingerprint。
- Reference绑定owner、scope、operation/branch、原消息身份、固定路由、wire/plan/fingerprint、执行fence与writer epoch。Checker只确认完全相同的持久化凭证。
- 当前fence的execution和branch都为Committed才Commit，都为Aborted才Rollback；缺失、Prepared、矛盾、不同fence/owner等均Unknown。lease过期本身不证明中止。DB故障由Checker实现返回Unknown。
- 半消息、本地事务、Broker二次确认使用三个独立结果类型；本地Committed与二次确认Unknown可以同时成立。它们均不表示消费效果或业务回执完成。

计划摘要的域为`rm-tx-plan-draft-v1\0`，每个字段使用8字节大端长度前缀；分支顺序被冻结。Go/Python共同读取[协议向量](../contracts/transaction-plans-draft-v1.json)，包括多分支、非ASCII身份和二进制wire。

本批测试证明不可变数据、身份绑定及状态判定，不证明原事务原子性、执行器隔离、资源关闭、Broker恢复窗口或业务验收。宿主迁移与Txn Sender仍须通过后续M2存储、适配和故障门禁。物理路由由infra审核清单提供，本包不创建资源，也不将旧NORMAL Topic原地改为TRANSACTION。

同一代计划的重试保留首次wire，但宿主可在旧执行被证明确认Aborted后显式授权新fence。每个执行记录以operation/fence/epoch区分并保持终态单调；旧Aborted不能改回Prepared。根计划的最新执行状态不能替代旧半消息的精确凭证。宿主授权、claim/CAS和迟到提交隔离仍须由后续数据库适配证明，Checker不创建新执行。

请求、计划代次、执行fence是三个不同身份。IAM等宿主若发现原计划依赖的业务版本已失效，可在证明旧计划从未提交且旧执行均已中止后，有界创建新的operation/计划代次；旧计划正文与终态不改写。并发首次请求仍选择同一个持久化赢家。MatchRequest只检查业务输入，不能代替宿主对当前活动计划、重建授权和版本CAS的检查。
