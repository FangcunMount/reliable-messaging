# M0 身份与内容指纹候选契约

这是基于当前源码构造的合成兼容样例，不是生产消息导出，不是已实现 SDK。文件可供 Go 与未来 Python 实现共享；M2 双存储验证后才冻结正式版本。

## 身份与字节规则

1. Outbox 身份为 producer + message_id + destination；consumer_id 属于独立消费关系，不写入发布者的内容指纹。同一消费者实例变更不改变逻辑消费身份。
2. 候选指纹使用 SHA-256，输入为 ASCII `rm-fingerprint-draft-v1` 加一个零字节，再按固定顺序拼接 9 个字段。每个字段先写 8 字节无符号大端长度，再写 UTF-8 原始字节：producer、message_id、destination、event_type、schema_version、tenant、content_type、occurred_at、payload。
3. 不规范化 JSON 空白或字段顺序，不重建 envelope、不删除未知字段。字段字节不同意味着指纹不同，即便解析出的 JSON 值相等；相同业务意图必须重用初次持久字节，而非重新生成。
4. 旧 source、追踪头、attempt_count、投递时间、claim_token 和 lease_until 不进入指纹；这些字段变化不产生新业务身份。原 source 仍按旧协议保留。
5. IAM producer 固定为 iam，QS 为 qs-server；旧 source=iam-outbox-relay／api-server 不是 producer 的自动推导规则。destination 使用固定路由映射，不能随 RabbitMQ 队列命名重造意图。未来迁 broker 前另验路由映射。
6. IAM 原载荷是业务 JSON，不要求其包含 message ID，ID 来自旧持久记录；QS envelope 必须与旧记录的 ID/type 对齐。IAM 授权版本实际载荷仅含 version；以固定的 scope:global 候选标记表达全局作用域，不能要求 payload 存在 tenant_id。QS 示例从 data.org_id 校验租户。作用域映射属于宿主适配器，不把所有事件强制设为同一租户字段。

## 样例与证据范围

identity-vectors.json 共 10 项：IAM 原消息、重复重放、内容／租户／格式变化；QS 含未知扩展字段的消息、重复重放、ID／租户／类型冲突。未知扩展字段作为完整 payload 字节参与指纹，不经 typed struct 重新编码。

预期摘要由 Python 标准库 hashlib/struct 计算，Go 标准库参考检查器独立重算并验证关系；测试会重新计算并核验摘要。这证明候选编码算法两种语言一致，不证明旧存储适配器或 MQ 发送已采用该算法。Go 检查器只是 M0 参考验证，未来不能作为生产通用 envelope 验证器直接复制。

```sh
go test ./tests/compatibility
```

源码依据：IAM pkg/outboxcore/core.go 使用 EncodePayload，infra/messaging/outbox_relay.go 使用持久 EventID 和原 Payload；QS 实际 component-base v0.6.9 的 pkg/eventcodec/codec.go 定义 id/eventType/occurredAt/aggregateType/aggregateID/data，pkg/outboxcore/core.go 的 DecodePendingEvent 尚未校验两份 ID 一致。

M1 将这些样例迁入独立仓库并添加真实适配测试；M2 还须验证数据库往返、历史数据扩展、原始字节发送、回滚和冲突隔离。未取得正式版本指纹的旧行不能仅写入本候选摘要后宣称迁移完成。

第 13 轮修正：首次 IAM 合成样例误沿用 store_test.go 辅助行的 tenant_id，而真实 domain/authz/policy/events.go 的 VersionChangedPayload 只有 version。现已按真实事件修正并重算预期摘要；先前通过结果仅证明错误样例内部一致，已被本轮复验替代。
