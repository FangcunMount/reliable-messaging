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
- NSQ raw-byte publisher and confirmed/rejected/unknown results.
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
