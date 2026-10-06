# Python 0.2 NSQ 适配器

本文描述当前 **0.2.0a2 源码**的可选 NSQ、wire 和回执型存储专有边界。正式制品与候选状态见[发布索引](../docs/releases/README.md)，不推导生产采用；0.1 核心没有 Broker adapter。安装 nsq extra，完整接线见 [Python接入](../docs/02-接入指南/Python接入.md)。

## 驱动、节点与生命周期

[NSQPublisher/NSQSubscriber](src/reliable_messaging/nsq.py)构造无 I/O；宿主显式 await start，使用共享 asyncio loop。先 Publisher 再 Subscriber；停止接纳和 Subscriber，再停止借用 Publisher/池。不运行 nsq.run、新 loop/线程/进程，不关闭共享 HTTP client。

精确 extra 由 [pyproject.toml](pyproject.toml)决定。pynsq 使用 Tornado；adapter 跟踪自己 heartbeat、reconnect/RDY timer 与 ready/connecting socket，仅关闭自己的 reader/writer。driver 自动丢弃关闭（max_tries=0）。

仅显式 nsqd TCP 地址，无动态 lookupd discovery。Publisher map 按原 source 借用；failure_ready(address, topic, channel)须通过显式 HTTP 核验相同 source 的 durable failure channel。首次发布前准备业务/失败 topic 与 channel，不猜 HTTP 端口、不漏发布节点，拓扑变更另验。

stop grace 不代表实际退出。handler 抑制取消、socket 未关闭或真实 Publisher callback 未完成时须保留资源/恢复责任。不能先关池再宣称排空。见[生命周期与恢复](../docs/01-核心设计/生命周期与恢复.md)。

## 持久提交与确认

- handler(Received)在业务/Inbox/回执 Outbox 提交后返回。
- failed_handler 先持久保存失败/hold；invalid_handler 按原 wire hash 隔离，不能相信未认证输入的 ID。
- 存储错误 REQ，取消保留未结算消息。物理尝试不替代宿主持久逻辑预算，重 PUB 不能取得无限新预算。
- publish 的 Confirmation.BROKER 仅为 PUB 层；超时 Unknown 保留 slot 直到真实 callback 结束，不得替代 deliver_durable 业务 callback。

通用确认分层见[投递与消费](../docs/01-核心设计/投递与消费.md)。模型调用未知结果、原任务授权和冻结配置由宿主判断。

## 回执型存储

[MySQLDurableOutbox](src/reliable_messaging/durable.py)借用宿主 Table，参考源为 [delivery/mysql/schema.sql](../delivery/mysql/schema.sql)。宿主原 bind(db).append(statement)保存意图，扫描/结算均要求活动非 autocommit 事务，由宿主提交；SDK 不迁移或关闭资源。

需回执行 PUB 后仍 awaiting_receipt，宿主认证并核对原身份/摘要才能 Confirm。迟到的 published/retry 技术回调不能把 confirmed/held 改回待投；需回执行仍可由经宿主核验的原业务回执从 held 结算为 confirmed。有序行等待更早 aggregate 决策。receipt-free final ACK 按 Broker 合同结束，没有 ack-of-ack。

rearm_ack 只重排原 final ACK，保持原 wire 与计数，不复活 held 或重建业务消息。0.2.0a2 成功 ACK PUB 不增失败预算，未知/失败仍计；需业务回执的未确认投递保持原有计数。旧计数可能含成功 ACK，不能自动归一/恢复。未确认/held 行及 body reference 不能自动删除。见[事务与Outbox](../docs/01-核心设计/事务与Outbox.md)。

## Wire、密钥与大消息

[wire.py](src/reliable_messaging/wire.py)实现 Revision 2 marker/checksum，checksum 不是发布者认证。[protected.py](src/reliable_messaging/protected.py)提供 rm-secure-v1：ES256 JWS，再 ECDH-ES+A256KW/A256GCM JWE，固定本地 P-256 kid map 并绑定 producer/destination/实际 topic 和消息上下文。

密钥由宿主提供，独立于 IAM/TLS，签名与加密分开。Seal 一次并保存原密文；技术重投不生成新 ID/时间/hash/密文。旧消息保留验证/解密 key，未知或撤销 key 经审计处理，不远程抓取或自动重封。

fits_nsq/Go protected.FitsNSQ 校验含最坏失败中转的 262144-byte 预算，异常文字不直接进入错误 metadata。超大正文用宿主不可变引用，认证后经授权只读 client 取回并核对身份/长度/摘要。SDK 不提供 blob 服务、不认识业务 protobuf、不扩大共享 NSQ 限制。见[安全与大消息](../docs/01-核心设计/安全与大消息.md)。

## 验证

[Python CI](../.github/workflows/python.yml)含一次性 MySQL/NSQ、原事务/进程、两语言 codec 和 installed-wheel，必需报告非空且零跳过。作业矩阵不能推出任意驱动组合受支持。

[Python实际MySQL测试](tests/test_durable_mysql.py)及[Go ACK预算测试](../tests/integration/ackbudget/mysql_test.go)验证两次未知后，同一 ACK 成功重放十六次计数仍为二，下一未知增至三，以及 held 抵抗重复/迟到成功。这是 adapter 证明，不是宿主上线或 Go M0～M6 验收。
