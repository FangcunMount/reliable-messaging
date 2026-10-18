# RocketMQ ordinary-message adapter

This adapter replaces the transport behind the existing host-owned Outbox and Relay. It does not implement RocketMQ transaction messages or change business receipt semantics. Constructors perform no I/O, driver construction or goroutine startup. Publisher.Start and Subscriber.Subscribe are explicit network boundaries; Close drains actual work and reports deadline failures.

The pinned upstream client is github.com/apache/rocketmq-clients/golang/v5 v5.1.4. TLS verifies the system trust store by default; hosts may supply an explicit CA pool through Config.TLS. No insecure certificate verification is added. Credentials and route bindings are host-provided; deployment is maintained in the infra repository.

RouteBinding version1 uses rmq_t_ plus the first32 hex digits of SHA256(logicalTopic), and rmq_g_ plus the first32 hex digits of SHA256(logicalTopic NUL logicalSubscription). Bindings must be explicit and canonical. No route is learned from message metadata. Broker names never replace the logical topic used by protected-wire authentication, message identity or fingerprinting. The original body is sent unchanged.

Confirmed requires an explicit nonempty send receipt. Timeouts, network errors and missing receipts remain Unknown. The configured server must independently prove synchronous flush and persistence; a confirmation does not prove consumer effect. Rejected covers local validation failures and explicit Broker validation/authentication/authorization refusal. Throttling and uncertain failures remain Unknown.

SimpleConsumer uses bounded receives, manual ACK, retry visibility and visibility renewal until the host deadline. A lost ACK reply does not settle the local delivery. After the delivery-attempt limit the host must durably record Failure before ACK; failed audit is retried. Broker native DLQ remains a monitored recovery obligation and is not a durable host business-failure record. Host handlers must respect cancellation; ignored cancellation remains active and can cause Close to time out.

Tests in this package use controlled drivers and do not prove real Broker behavior. examples/rocketmq and python/examples/rocketmq_probe.py target explicitly provisioned disposable infra routes and verify actual send/receive evidence. They do not provision business queues or authorize recovery. Production cutover, database transaction regression, load and fault evidence remain required before release/adoption acceptance.
