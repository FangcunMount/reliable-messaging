# Historical component-base NSQ envelope fixtures

M6-04A input contract, captured 2026-09-27 (UTC+8). These are synthetic messages, not production records or credentials. The same fixed `messaging.Message` was encoded by `EncodeMessagePayload` using IAM's pinned `component-base v0.6.1` and qs-server's pinned `v0.6.11`. The service `go.mod` versions were checked against production image commits at capture time: IAM `4d644088254bd37f252fb98d73ffd722772e34d9` on serverB; QS API `b75c8b03db373f2a2624029026d5375e0375b20a` on serverA. Worker deployment and all topic/channel bindings require a separate runtime check.

Input: UUID `550e8400-e29b-41d4-a716-446655440000`, UTF-8 payload `{"event_type":"policy.changed","subject":"青岛店"}`, metadata `source=iam-outbox-relay`, `event_type=policy.changed`, `trace_id=contract-fixture`. The source string makes the fixture IAM-shaped; the codec contract is generic. The trailing repository newline is not part of the emitted wire bytes.

| Encoder / decoder | v0.6.1 envelope | v0.6.11 envelope | v0.6.11 envelope with bad checksum |
|---|---|---|---|
| v0.6.1 decoder | accepts | accepts, ignores revision/checksum | accepts, ignores checksum |
| v0.6.11 decoder | accepts revision 1 | accepts revision 2 after checksum verification | recognizes envelope and returns error |

`component-base-v0.6.1-envelope.json` has no revision or checksum. `component-base-v0.6.11-envelope.json` has revision 2 and checksum. `component-base-v0.6.11-bad-checksum.json` changes one checksum character only; it is a negative decoding fixture. The matrix was run against each released module in the host module context, not inferred from the local `component-base` checkout.

The SDK does **not** yet expose an envelope decoder or subscriber. Freezing these bytes and behaviors does not prove M6-04B or production compatibility. A replacement decoder must accept both valid revisions, reject a recognized corrupted revision 2, and keep unmarked raw payload behavior explicit. It must preserve application UUID, metadata, payload bytes, and the existing topic/channel and settlement contracts. The old v0.6.1 decoder's checksum blindness is documented for rollback analysis, not a behavior to recreate in the new decoder.
