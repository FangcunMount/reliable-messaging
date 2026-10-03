# Python NSQ candidate (0.2.0a1)

This is an embedded, optional adapter. Install the `nsq` extra. It does not own
business transactions, tables, connections, model retries, keys or deployment.
This document describes candidate source, not a formal release or production adoption.

## Lifecycle and resources

Create `NSQPublisher(address)` and `NSQSubscriber(topic, channel, ...)` without I/O.
The host awaits `start`, owns the shared asyncio loop, and explicitly awaits
`stop(grace_seconds=...)` and `wait`. Start the publisher before the subscriber;
stop admission and subscribers before stopping borrowed publishers and database pools.
No `nsq.run`, new event loop, process or thread is introduced by the adapter.

The pinned official pynsq 0.9.1 client uses Tornado. The adapter tracks its heartbeat,
reconnect/RDY timers and connecting/ready sockets. It closes only its own readers and
writers, never the shared event loop or HTTP client. Driver auto-discard is disabled
(`max_tries=0`). Explicit NSQD TCP addresses are required; this minimum version does
not implement dynamic lookupd discovery. Changing topology requires a separate proof.

`failure_ready(address, topic, channel)` must verify the durable failure channel is
connected on that exact source NSQD, using explicitly configured HTTP endpoints.
Prepare business and failure topics/channels before the first publish. Do not infer
HTTP ports or silently accept missing source nodes. Publisher maps are borrowed.

## Commit boundary and confirmation

- `handler(Received)` returns only after host business/Inbox/receipt-Outbox commit.
- `failed_handler(bytes)` persists the failure/hold before returning.
- `invalid_handler(bytes, code)` durably quarantines by raw-wire hash, without trusting
  unauthenticated message IDs, before returning.
- A storage error REQs; cancellation leaves the message unsettled; no callback invokes
  a model or grants business retry permission. Exception text is not put in handoffs.
- `publish` returns `Confirmation.BROKER`. Timeout is Unknown and retains the in-flight
  slot until the real callback finishes. Do not pass it to `deliver_durable`.

`MySQLDurableOutbox` uses a separate host table described in `delivery/mysql/schema.sql`.
The host appends via the existing `bind(db).append(statement)` in its original active
transaction. Every scan/settlement also borrows an active non-autocommit transaction.
The adapter never begins, commits, rolls back, migrates or closes resources.

`published` keeps receipt-requiring rows in `awaiting_receipt`. Only a host-verified
authenticated receipt may call `confirm`. Late publisher/ retry callbacks cannot undo
confirmation or a technical hold. Ordered records wait for earlier aggregate decisions.
`rearm_ack` is limited to receipt-free final acknowledgements and preserves the original
wire; there is no ack-of-ack and no automatic rearm of business execution messages.

NSQ attempt counts are physical delivery counts. The host must also persist logical
failure budgets; re-PUB must not obtain a new unbounded processing budget. This adapter
does not replace that host policy. Unconfirmed/held rows and body references must not
be automatically purged. Scan after restart even without a wake notification.

## Wire, keys and oversized bodies

`wire.encode/decode` implements the existing Revision 2 marker/checksum. Checksum is
not publisher authentication. `protected.seal/open_message` implements rm-secure-v1:
ES256 JWS followed by ECDH-ES+A256KW / A256GCM JWE, P-256 keys with fixed local `kid`
maps. Routing context binds producer, destination, actual topic, identity and metadata.
SDK keys are separate from IAM/TLS and separate between signing and encryption.

Seal once and persist those bytes. No technical replay regenerates IDs, time, payload,
hashes or ciphertext. Keep old decrypt/verify keys while pending history remains;
revoked/unknown keys are held for audited handling, not fetched remotely or resealed.

`fits_nsq` / Go `protected.FitsNSQ` check 262144 bytes including the worst failure
handoff. Error metadata has a reserved bound; driver exception text must not be copied.
An oversize body needs an immutable host-database reference and a configured,
authorized read-only mTLS client. The SDK does not implement a blob service or know
business protobuf types. Fetch only after authentication and verify identity, length
and hash. Never expand global NSQ limits to hide a host wire-size error.

## Required proof

CI requires real disposable MySQL and NSQ endpoints, nonempty reports, zero skips,
original transaction and process tests, both-language codecs, and installed-wheel
checks. Test/CI success is distinct from release, normal host-image health and business
acceptance. Current supported candidate combinations are defined by the workflow;
no other Python/client/Broker/database combination is implied.
