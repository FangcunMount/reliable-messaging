# NSQ publisher lifecycle (provisional)

The host constructs/configures a `go-nsq v1.1.0` Producer and lends it to `New`. Logical destinations map explicitly to physical topics; this adapter does not create subscriptions or change stored bytes. The constructor starts no work. Configure bounded driver dial/read/write timeouts.

`Publish` returns `Confirmed` only on a nil driver result. Missing/invalid local routes are `Rejected`; driver errors and context expiry are conservatively `Unknown`, because the broker may have received the message. NSQ acceptance does not prove consumer handling or synchronous disk durability. The adapter does not retry internally.

The pinned driver's Publish call has no context parameter. Each admitted call runs with one bounded in-flight slot; context expiry returns Unknown but retains that slot until the actual call finishes. Repeated timeouts therefore cannot create unlimited background sends. The host should choose a limit aligned with Relay concurrency and account for possible duplicates while an earlier unknown send is still in flight.

Shutdown order:

1. Cancel Relay admission and wait for Relay.Run to return.
2. Call Publisher.Drain with a bounded context. Drain permanently stops new admission and waits for actual driver calls.
3. When drained, stop the host-owned producer, then close host databases.
4. If Drain times out, keep it visible as an incomplete shutdown. The host may call producer.Stop to interrupt remaining driver activity, then recheck Drain. A timeout is never successful drain evidence.

Draining only Relay is insufficient to prove that the non-context-aware driver has stopped. No producer ownership is silently transferred to the SDK. Unit tests distinguish caller completion from driver completion; real integration drops the NSQ PUB response after broker acceptance to test Unknown plus byte-preserving duplicate delivery.

## Subscription binding candidate (M6-04B)

`NewSubscription` only validates a topic/channel, bounded attempt policy and injected handlers. It creates no connection, producer, consumer or goroutine. The host uses `ConsumerConfig` (which disables go-nsq's own attempt cutoff), creates two host-owned consumers, attaches `FailureHandler` to the historical failure topic/channel and connects that consumer **before** attaching/connecting `BusinessHandler` to the business topic/channel. The host also supplies a `Handoff` implementation that confirms the failure consumer is connected to the source nsqd and publishes to that same nsqd; only a confirmed handoff may finish the original. The SDK currently has no production `Handoff` implementation or complete Subscriber/EventBus lifecycle API.

`transport.Delivery` exposes application and physical identities separately. An explicit Ack/Nack takes precedence over a handler return; otherwise nil acknowledges and an error requeues. Once the budget is exhausted, the business handler is skipped and the original is only finished after a confirmed failure handoff. A lost handoff confirmation can create a duplicate failure record. The host's failed handler must durably record the handoff before returning nil; the SDK cannot infer that from a callback. Business idempotency and governance remain with the host. The focused isolated NSQ test demonstrates two physical failure records after an accepted handoff with a simulated lost confirmation; it does not prove MySQL audit, broker fsync, service cutover or complete fault recovery.
