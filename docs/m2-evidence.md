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


## M2-04: actual service transaction and historical-row comparison

- IAM [draft PR #93](https://github.com/FangcunMount/iam/pull/93), test commit `7c262f0e` on baseline `24dbe924`: actual authz UoW, role repository, policy-version repository and historical Outbox stager plus SDK BindGORM under the original transaction. Commit preserves all records; host abort and SDK content conflict roll back role/version/old row/SDK intent. Original IAM wire remains `{"version":2}`. SDK claim/confirmation preserves the mapped identity/fingerprint.
- qs-server [draft PR #127](https://github.com/FangcunMount/qs-server/pull/127), test commit `7adb22d5b` on baseline `5573735ae`: actual NewMongoRunner and historical eventoutbox Store plus SDK session binding. Two callback executions leave one committed synthetic business record, one old event and one SDK intent. Admission acquire/release stays once per transaction. Host abort/SDK conflict fully roll back. Unknown top-level envelope extension remains byte-identical through SDK claim/confirmation.

Both required tagged tests ran against SDK `42fa4c3`, real isolated MySQL 8.0.44 / Mongo 7.0.37, and passed. Temporary Go workspaces avoid changing service module dependencies; scripts accept no production connection strings and cleaned their own containers. Service main checkouts and production configuration were untouched. SDK 42fa4c3 passed all four CI jobs.

These test-only drafts use an extra SDK table/collection for comparison, not a proposed production dual-publish scheme. IAM's proof maps stored created_at once because the old row lacks occurred_at; qs-server reads persisted envelope metadata while retaining raw bytes. Final historical identity policy and schema/claim/status/token migration remain open. qs-server uses a synthetic business record; full AnswerSheet acceptance is later. Normal service CI does not run these tagged cross-repo proofs. API remains provisional until remaining compatibility/fault gates pass.

## M2-06: real process death with MySQL, Relay and NSQ

`TestRelayProcessCrashRecovery` launches a separate copy of the integration binary, using the actual MySQL Store, Relay and NSQ adapter. A barrier freezes it immediately after a durable claim or after NSQ confirmation but before the DB confirmation update. The parent sends SIGKILL and checks the actual exit signal. A fresh Relay waits for natural database-time lease expiry; there is no manual mutation of the lease or message.

Both points passed against real MySQL 8.0.44 / NSQ 1.3.0. Before-publish death recovered the original ID with two claim attempts and one observed physical delivery. Before-writeback death recovered the original ID with two claim attempts and two physical deliveries. Exact payload bytes matched. A deliberately idempotent fixture consumer stored one row/effect in both cases. This is a combined transport/recovery proof, not evidence that existing IAM or qs-server consumers are idempotent. It does not prove broker/node power-loss durability.

The test owns a separate temporary DB per case and kills/reaps its child process on failures. Required infrastructure remains mandatory. Standard check/race and lint are separate from the tagged integration binary; the actual crash experiment is not represented as a race-instrumented run.

## Service draft CI follow-up

IAM #93 at 7c262f0e passed Analyze, Lint, Run Tests and Build. SDK 56f8eea passed all four jobs. qs-server #127 initially failed Docs Hygiene because the new sidecar was missing from document-closure.json and its source baseline had moved. Commit 42d10cb82 registers the test-only document as needs_review and updates the source baseline; local `make docs-facts` passed. Its new CI remains separate. Normal service jobs do not execute cross-repo tagged transaction proofs.


## M2-05: original IAM consumer and qs-server failure-hold compatibility

IAM #93 commit 038df0b2 adds real NSQ/MySQL consumer proof against SDK d13ee10. Two actual policypublication handlers and immutable runtimes use MySQLSource and separate test channels. A TCP proxy drops each first FIN after successful processing; broker redelivery preserves the NSQ message ID and increments attempts. Both instances avoid reloading already-loaded versions, reject regression from older versions, and recover a deliberately unpublished DB version via the original Reconcile. Invalid version payloads remain errors. Separate test channels prove broadcast but not production ephemeral-channel lifecycle.

The first ACK-loss fixture disabled auto-response locally and failed shutdown because go-nsq retained in-flight accounting. It was replaced by dropping FIN on the wire; the corrected run passed and both consumers/proxies drained. This is actual broker ACK-loss/redelivery, not two manual handler calls.

qs-server #127 commit 583739a89 uses the original dispatch settlement handler and mysqlRetryEventHoldStore with the exact 000050 schema in real MySQL. Injected dispatcher pause and ACK failure prove ACK occurs after the durable hold. Redelivery does not reset manual_required, replay count, manual request or original bytes. A real trigger rejects the next hold: the original handler NACKs/returns error without acknowledging an unpersisted record. Dispatcher/ACK boundaries are controlled injections; this does not prove the Assessment intake's business idempotency or network ACK loss for qs-server.

Both extended isolated scripts passed and removed resources. Service module files/production wiring are unchanged. New service CI remains separate; normal jobs exclude these cross-repository tagged proofs. qs-server document registration/source baseline is maintained alongside the additional proof, with local docs-facts passing.


## Database-relative retry scheduling

The provisional Store.Retry API now takes a positive time.Duration rather than an absolute host timestamp. Relay forwards the host policy delay unchanged. MySQL computes UTC_TIMESTAMP(6) plus a ceiling-rounded microsecond delay inside its fenced UPDATE; Mongo computes $$NOW plus a ceiling-rounded millisecond delay in an update pipeline. Neither path reads the Relay host wall clock. The pipeline treats error codes as literal data, including a leading dollar sign, and preserves token/version/state/expiry predicates and lease cleanup.

Real-storage tests reject zero/negative delays without changing ownership, compare persisted due times with database time, preserve a literal dollar-prefixed code, exclude future records and continue to reject stale writers. Existing host-policy ownership and retry budgets remain unchanged. This closes the demonstrated host/DB skew dependency for relative delivery retry only: initial Appender due times are still explicit business scheduling instants; database wall-clock steps/failover require an operational policy, and this is not a clock-synchronization test. The earlier absolute-Retry entries above describe historical revisions, superseded here. No stable SDK version or production adapter has been released.

Validation: make check (including race), make lint, integration-tag vet and the complete isolated MySQL/Mongo/NSQ suite passed. Existing SIGKILL recovery, unknown commit, publication confirmation loss and stale-claim tests remain passing; isolated resources were removed. New CI is checked separately.
