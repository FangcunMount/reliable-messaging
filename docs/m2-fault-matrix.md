# M2 fault-matrix audit

This audit maps the implementation plan's section 13 to concrete evidence. It is not an approval. Read alongside [candidate review](m2-review.md) and [task gates](m2-acceptance-gaps.md). “Covered” below is restricted to the named test boundary; later service composition and production gates remain required.

| Required scenario | Evidence / current result | Boundary and next gate |
| --- | --- | --- |
| Business rollback | `tests/integration/mysql_test.go: TestMySQLTransactionAndFencing`, `gorm_test.go: TestGORMOriginalTransactionBridge`, `mongo_test.go: TestMongoOriginalTransactionAndReentry`; original IAM/QS transaction proofs | Covered for tested original transactions; service adapter assembly remains M3/M4 |
| Exit after committed intent | `crash_test.go: TestRelayProcessCrashRecovery`, child SIGKILL after claim before publish, replacement Relay after lease | Real committed SQL intent and process crash covered; not Mongo service process recovery |
| Broker accepted, writeback failed | `mysql_test.go: TestRelayRecoversRealDatabaseWriteFailure` plus crash test before confirmation write | Trigger rejection uses a publisher double; separate real NSQ/process test covers accepted-before-write crash. Do not conflate their boundaries |
| Lost publication response | `nsq_test.go: TestNSQLostConfirmationPreservesWire` | Real PUB acceptance with response dropped; Unknown followed by original-byte retransmission |
| Old Relay returns late | `late_relay_test.go: TestLateRelayCannotOverwriteRecoveredClaim` | Two actual Relays and real SQL; delayed publisher is a double. New claim wins; stale write observable |
| ACK/FIN lost after consumer commit | IAM #93 original policy consumer; QS #127 FIN-drop original Worker/gRPC/Journey/MySQL proof | Actual network redelivery and durable dedup boundaries covered. Full QS production dispatcher/producer composition remains M4 |
| Isolation database failure | QS #127 durable RetryEventHoldStore failure-insertion proof | Real SQL persistence failure leads to NACK/error in settlement fixture; not network FIN test of that particular hold path |
| Same identity, different content/scope | `tests/compatibility/identity_test.go`; real MySQL/Mongo duplicate identity conflicts | Contract vectors include scope/content/format distinctions; preserve service historical mappings in M3/M4 |
| Out-of-order, duplicate, missing versions | IAM #93 policy version duplicate/old-version/reconciliation proof | Applicable versioned IAM behavior; SDK does not impose generic global ordering |
| Redis ready-index loss | Not an SDK dependency or a M2 integration proof | Required in M4/M5 with actual QS database scanner and Redis index; cannot claim passed |
| Graceful broker restart | `tests/integration/brokerrestart`, `scripts/integration.sh` | Same-volume actual restart, 10/10 original application identities and bytes consumed |
| Abrupt broker crash / data loss | Same fixture, isolated crash topic: confirmed memory count 10, SIGKILL, same-volume restart, count 0 | Observed loss, NOT successful recovery. Per-flow compensation/fault-model acceptance required before rollout; permanent-node loss not tested |
| qs-ai accepted task notification lost | Not tested here | M7; original task owner and accepted baseline required |
| Model sent without durable receipt | Not tested here; no real model calls | M7 must prove zero automatically added calls and original unknown-result handling |
| Model response already durable | Not tested here | M7 must prove response/config/fingerprint reuse |
| Candidate cancellation capacity | Not tested here | M7 under original execution/recovery owner |
| Cutover and rollback | IAM historical SQL prototype demonstrates unsafe unchanged legacy-writer overlap | M3–M6 must rehearse exclusive handoff, backlog and rollback; no production acceptance |
| Go/Python interoperability | Go verifies draft reference fingerprint vectors generated independently | Not Python SDK interoperability; real cross-language integration belongs to M7 |

## Cross-cutting gates

- IDs, payloads and durable states are checked within the tests; a shared count alone is not a business-effect proof. The crash fixture's dedup consumer is synthetic; original-service FIN-loss proofs are separate.
- SDK lease/process recovery uses short explicit test deadlines. These are not production SLOs. Service recovery thresholds, throughput, backlog drainage and DB load must be fixed from actual flow baselines before cutover.
- `relay/relay_test.go` proves bounded admission, drain and single-runner behavior with cooperative doubles. `transport/nsq/publisher_test.go` separately proves timeout slot retention and adapter drain. Arbitrary noncooperative host callbacks cannot be forcibly terminated; host shutdown must preserve DB/producer ownership until drain.
- This audit does not move any failed or absent guarantee into “passed.” The confirmed-memory loss is a release constraint. Passing M2 mechanism tests does not authorize a stronger end-to-end reliability claim.
