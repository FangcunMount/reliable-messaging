# Go NSQ 适配器

本包适配固定 go-nsq 驱动，提供发布、订阅、失败中转与显式 channel 准备。通用确认/重投见[投递与消费](../../docs/01-核心设计/投递与消费.md)，宿主接线见 [Go 接入](../../docs/02-接入指南/Go接入.md)。宿主拥有业务路由、Broker 拓扑、幂等、失败审计和运维权限。

## 发布与资源所有权

| 入口 | 初始化 | 关闭责任 |
|---|---|---|
| [New](publisher.go) | 借用原 Producer，不连接、不启动 goroutine | Drain 不 Stop Producer，宿主最后关闭 |
| [NewManagedPublisher](managed_publisher.go) | 显式构造并连接/Ping nsqd，失败清理新 Producer | SDK 自持有 Producer，宿主显式 Close |

两者有有界 in-flight 和相同结果分类，驱动须配置有限 dial/read/write timeout。Publish 按复制的 destination 路由发原 payload；PublishRaw 向显式 topic 发已编码字节。它们不自动生成 envelope、认证或内部重试。

nil driver 结果仅为 NSQ PUB Confirmed；非法本地路由/topic/空 raw body 为 Rejected；driver 错误和 context expiry 保守为 Unknown。PUB 不证明消费者持久接单、业务完成或同步磁盘持久性。Unknown 后保留原应用身份/wire，外部副作用重试许可仍属于宿主。

go-nsq Publish 无 context 参数。已接纳调用占一个 slot；调用方超时后 slot 保留至实际调用结束，避免无限后台发送。稍后仍可能投递，因此仅等待 Relay 不能证明 Publisher 退出。

## 停止

1. 宿主停止新接纳，取消 Relay 并等待 Run 返回。
2. 借用型 Drain(ctx)，成功后由宿主 Stop Producer；自持有型 Close(ctx)，排空后停止其 Producer。
3. 所有 handler/Relay/driver 实际停止后，才关闭其需用的宿主数据库等资源。

超时是未完成停止。借用型可明确 Stop 中断后再检查 Drain；自持有型可明确 Interrupt，再检查 Close。中断中的 publish 结果未知。超时保留对象所有权，不能丢弃对象后声称资源已关闭。

## 订阅与失败中转

[NewSubscription](subscription.go) 是纯绑定，[NewSubscriber](subscriber.go) 也不连接，Subscribe 才显式工作。宿主选择直接 nsqd 或 lookupd；每条订阅先接失败 consumer，再接业务 consumer。业务 topic 未注册时，lookupd bootstrap 选一个活动节点准备失败路径；动态 source 在 terminal handoff 前先绑定失败 consumer。

DirectHandoff 借用失败 consumer，在显式工作期间创建 source nsqd Producer，Close 排空/停止这些 Producer；borrowed consumer 归 Subscriber/宿主。驱动默认超尝试丢弃关闭，由 SDK 有界政策处理。

Delivery 分开应用 ID 与物理 TransportID。显式 Ack/Nack 优先；未显式结算时 nil 返回 Ack，error 返回 Nack。宿主 handler 必须持久幂等处理/接单提交后才返回 nil，failed handler 先持久保存中转再返回。预算耗尽跳过业务 handler，原消息仅在中转确认后 FIN；丢确认形成重复失败记录，失败审计也需去重。

Close 在等待连接中的 registration 前停止接纳；超时之后 bounded driver call 可能继续清理，须再次 Close 核验。Subscribe 失败且 cleanup 未排空时保留 partial consumer/handoff，原对象仍负责停止；失败返回不能证明全停，也不能立即换实例重复注册。DirectHandoff Close 保留同样责任。见[生命周期与恢复](../../docs/01-核心设计/生命周期与恢复.md)。

## 首次发布前的 channel 准备

[NewProvisioner](provisioner.go) 借用 HTTP client 和显式 nsqd HTTP URLs，构造无网络；EnsureChannel 在每个声明节点先创建 topic 再创建 durable channel。不从 TCP 推断 HTTP 端口，不把 partial failure 当就绪；发布节点变更要重新准备。topic/catalog、广播与工作分摊的 channel 选择由宿主决定。

## 验证入口

- [丢 PUB 确认与原 wire](../../tests/integration/nsq_test.go)、[自持有 Producer](managed_publisher_integration_test.go)。
- [订阅](../../tests/integration/nsq_subscription_test.go)、[动态节点](../../tests/integration/nsq_dynamic_node_test.go)、[首次 channel 准备](../../tests/integration/nsq_provisioner_test.go)。
- [真实中转丢确认](handoff_disconnect_integration_test.go)、[MySQL 失败审计](../../tests/integration/nsq_failure_audit_test.go)。

这些证明已测连接、重投、排空、审计边界，不证明 Broker fsync、宿主切换或业务恢复。支持范围见[兼容与升级](../../docs/03-维护与验证/兼容与升级.md)和[测试与故障验证](../../docs/03-维护与验证/测试与故障验证.md)。
