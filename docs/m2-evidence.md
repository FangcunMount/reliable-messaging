# M2 implementation evidence

M2 is in progress, not accepted. Public APIs are provisional until real MySQL and Mongo original-transaction proofs pass.

## M2-01: standard MySQL Outbox

Implemented an immutable, copied-payload Message using the M0 draft fingerprint; a typed original `*sql.Tx` Appender; explicit host-applied schema; bounded claiming with row locks and SKIP LOCKED; database-clock leases; conditional confirmation, retry and quarantine. Identity duplicates preserve the existing record, and differing immutable content returns `ErrConflict`. The host must roll back its business transaction on Append error. The adapter never commits or rolls back the host transaction.

The standard `rm_outbox` schema is for the isolated SDK proof. It does not replace IAM or qs-server historical schemas. `database/sql` MySQL handles must enable `parseTime=true` and UTC location. Claim/Confirm use database UTC. Due timestamps are host-supplied; clock skew in future Relay scheduling still needs a policy.

Validation covers actual SDK fingerprints against the existing 10 reference vectors, payload ownership, absent transaction rejection, and real MySQL 8.0.44 transactions. Host-specific wire validation remains outside the generic Message API.

Real integration cases: business/intent commit and rollback; same-identity replay and conflict; exclusive live lease; forced lease expiry and reclamation; stale Confirm/Retry/Quarantine rejection; delayed retry; terminal publication; concurrent claimers; preserved corruption quarantine without blocking valid work. Lease expiry is a direct isolated-row mutation, not a process-crash experiment.

The first integration attempt uncovered a pre-existing readiness race: the MySQL initialization-only socket server satisfied the health check before the final server started. Health now requires a successful TCP SQL query, which that temporary server cannot accept. The failed run was cleaned up; production resources were untouched.

## Remaining M2 gates

- Complete Relay acceptance against actual NSQ timeout/shutdown behavior; verify database/host clock skew policy.
- Extend NSQ proof to the combined durable Relay/recovery loop and original consumer idempotency.
- Actual IAM UoW/old-schema and qs-server Mongo transaction adapters and examples, including callback reentry and unknown commit.
- Consumer compatibility/metrics and real crash, lost-confirm and writeback-fault experiments.
- API review after both storage proofs; service acceptance belongs to later milestones.

## Current local results (2026-09-22)

`make check`, `make lint` (0 issues), and `make integration` passed after the readiness fix and the concurrent-claim/corruption cases were added. The run created and removed only its randomized Compose project. CI results are separate and pending. Existing Mongo/NSQ probes still validate infrastructure, not SDK adapters.


## M2-02: bounded Relay and real writeback rejection

Relay now admits at most the configured concurrency per batch, publishes outside the claim transaction, stops admission on cancellation and drains admitted work. Publish/write operations have separate deadlines; constructors acquire no resources. Host retry policy explicitly chooses delay or quarantine, and bounded observer categories expose publish outcomes, failed scans, write failures and stale writes. Callbacks must be fast/concurrency-safe and publishers must honor context; no unbounded timeout wrapper goroutines are used. Only one Run per Relay instance is admitted.

Race-tested unit scenarios cover bounded admission/drain, concurrent Run rejection, confirmed/unknown/rejected outcomes, stale writes, DB errors and publisher timeouts. The lifecycle example now runs the actual SDK Relay with explicit in-memory doubles.

`TestRelayRecoversRealDatabaseWriteFailure` uses a MySQL trigger to reject publication confirmation updates. The durable row stays publishing; after removing the fault and expiring its lease, the Relay sends the exact same identity/fingerprint/payload and reaches published. This is actual database error/recovery evidence with an in-process publisher double, not a broker ACK-loss or process-crash proof.

Local `make check`, `make lint` and isolated integration passed for Relay and the fault test. After replacing the lifecycle example, its execution and vet also passed. The prior commit cf2e088 passed all four push/PR CI jobs. New commit CI remains separate. Host-supplied Retry due time still uses host UTC; clock-skew policy remains open before API acceptance.

## M2-03: pinned NSQ adapter

Added an adapter around host-owned go-nsq v1.1.0 with explicit copied route mapping and raw-byte preservation. Constructors do not connect/start work. Local invalid routing is rejected; nil driver results confirm broker acceptance; driver errors/timeouts remain unknown. No internal retry or business ACK claim is made.

Source inspection of pinned producer.go showed that Publish/PublishAsync do not accept context, and Async can itself block during connect/admission. The adapter therefore bounds actual underlying calls, retaining capacity after caller timeout until the driver finishes. Publisher.Drain is an explicit second shutdown step after Relay.Run returns, before the host closes the producer/database. A drain timeout remains an incomplete shutdown, never success.

Unit/race tests cover preserved route/bytes, conservative outcome classification, calls after drain, timeout capacity retention and observable incomplete drain. The real NSQ test uses a TCP proxy to drop the PUB OK after acceptance, then retries via a direct connection and compares both consumed bodies. Test setup must create topic before channel and use a broker-valid heartbeat below the driver read timeout; initial setup failures are not fault-test success.

Real NSQ 1.3.0 lost-confirmation test passed after correcting setup. Both physical deliveries retained exact original bytes, first attempt was Unknown and retry Confirmed. The full isolated integration run passed and cleaned its resources. This is not evidence of host consumer idempotency, process-crash recovery or synchronous disk durability.

## M2-04: GORM and Mongo transaction bridge proofs

Source checks reconfirmed IAM authz uses its GORM UoW and qs-server wraps mongo Session.WithTransaction. Added `BindGORM` for the existing GORM 1.30.0 plain/prepared original SQL transaction handles; ordinary DB handles and unsupported wrappers fail closed. Real MySQL tests passed original business+intent commit/rollback for both modes and rejected completed-transaction reuse. This is not yet an invocation of IAM's actual UoW or its old schema.

Added a provisional Mongo v1.17.6 original-session Appender. Mongo v1 lacks a stable transaction-state accessor, so the adapter confines one explicit XSession compatibility dependency to storage/mongo. A SessionContext without an active transaction and retained appenders after commit are rejected. Standard proof documents preserve the identity tuple, original bytes and fingerprint, including callback reentry. qs-server historical fields, claim token changes and the numeric-only Claim ID interface remain open before API acceptance.

Replica-set tests for original transaction commit/rollback, callback retry, duplicate identity/conflict and payload preservation passed. The callback retry is induced through a labeled callback error after real writes, not through an actual primary election. Commit-unknown failpoint evidence is recorded separately after execution.


The real commit-unknown experiment passed: Mongo 7.0.37 `failCommand` injected a write-concern error labeled UnknownTransactionCommitResult on the first commit response. Command monitoring observed two commit commands, one injected unknown response and exactly one business callback; both collections held one committed record. This proves the pinned driver's resolved-unknown commit retry path, not indefinitely unresolved commits or failover recovery. Test commands are enabled only on this invocation's isolated Mongo container. The first attempt failed because RunCommand requires an ordered document; the corrected run passed all integration cases and cleaned resources.

Local check/race and lint passed for the GORM/Mongo implementation; full isolated integration passed after adding commit-unknown injection. Previous head cb053f0 passed all four CI jobs. This is still M2 work in progress: actual IAM UoW/old schema, qs-server historical document/token mapping, shared Claim identity, unresolved commit reconciliation and end-to-end consumer fault proofs are not accepted.


## M2-04: shared claim identity and Mongo claim/recovery proof

Claim.RecordID changed from SQL uint64 to an opaque adapter-owned string. Relay does not parse it. MySQL validates canonical positive decimal keys before conditional writes, preventing SQL's coercion of malformed string keys. Mongo encodes its standard BSON identity tuple as an opaque key; it does not require a synthetic numeric identifier or expose a driver type in the core contract.

The standard Mongo Store now atomically claims due/expired rows, derives lease expiry from database $$NOW, and confirms/retries/quarantines only when key, token, version, publishing state and unexpired lease all match. Index installation remains explicit and constructors are inert. Production query plans for the $$NOW predicates remain a rollout gate.

Real Mongo 7.0.37 tests passed live-lease exclusion, expiry/reclaim, stale Confirm/Retry/Quarantine, future retry, due retry, terminal publication, two concurrent claimers over six records, and preserved-corruption quarantine. Existing MySQL/GORM, Mongo transaction/unknown-commit and NSQ lost-confirmation integrations also passed. Local check/race and lint passed; the added SQL malformed-key test has its own focused race run. These proofs apply to the standard SDK schemas, not historical IAM/qs-server collections/tables. Previous numeric-only identity limitation is resolved; historical field/status/token compatibility remains open.
