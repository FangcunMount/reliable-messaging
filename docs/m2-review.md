# M2 candidate review

M2 remains in progress and requires the project initiator's final review. This package proposes a reusable delivery foundation; it does not approve service cutover, stable API publication, or production reliability. The task-by-task evidence and open gates are in [the acceptance index](m2-acceptance-gaps.md), with detailed reproduction in [M2 evidence](m2-evidence.md).

## Review scope

| Candidate | Review scope | Excluded from this approval |
| --- | --- | --- |
| [SDK #2](https://github.com/FangcunMount/reliable-messaging/pull/2) | Immutable intent, original-transaction appenders, MySQL/Mongo stores, bounded Relay, NSQ adapter and isolated fault tests | Stable release, automatic DDL, production resource ownership or generic business compensation |
| [IAM #93](https://github.com/FangcunMount/iam/pull/93) at 8aae3df4 | Actual UoW, old wire, original consumer/reconciliation and historical-table fencing feasibility proofs | Deployable historical Store, migration, production wiring or parallel old/new Relay operation |
| [qs-server #127](https://github.com/FangcunMount/qs-server/pull/127) at 606eb3226 | Original transaction/consumer/hold proofs plus atomic pending-to-submitted host fix | SDK production wiring, model calls or qs-ai execution/recovery changes |

## Candidate contracts to review

- `message.Message`: producer + application ID + logical destination identify delivery. Immutable metadata and original payload bytes participate in the draft fingerprint. Same identity with changed scope/content conflicts; retry never creates a new business identity. Payload access returns copies. No wire envelope is imposed on historical consumers.
- Appenders borrow an existing host transaction. MySQL requires the original SQL transaction (including verified GORM binding); Mongo requires the active session transaction. The host commits or rolls back. Constructors do not migrate schemas or start background work.
- `outbox.Store`: opaque record key; claims carry token, version, attempt count and lease. Conditional mutations check state, token, version and database-time lease validity. `Retry` takes a positive duration relative to the database clock, rounded up to its precision. Initial scheduled time remains host-supplied. Database wall-clock discontinuities remain an operational assumption.
- `transport.Publisher`: outcomes are confirmed, rejected or unknown. NSQ confirmed means accepted by the broker protocol, not consumer completion or crash-safe persistence. Uncertain calls retain their original identity and bytes.
- `relay.Relay`: explicit bounded concurrency and timeouts; host supplies retry/quarantine policy and observation. Cancellation stops admission and drains admitted work. Host callbacks must cooperate; Go cannot forcibly terminate an arbitrary hung callback. NSQ in-flight driver slots remain occupied until the actual call finishes; the host drains before resource shutdown.
- Attempts count claims in the SDK. IAM's historical counter counts failed publication attempts. The service adapter must preserve this distinction and the existing retry budget, rather than silently applying SDK counts as business policy.

These APIs remain provisional pending review. Cross-language fingerprint fixtures provide a draft contract, not a tested Python implementation.

## Material findings and behavior changes

**NSQ abrupt termination loses confirmed memory messages under the tested configuration.** The isolated fixture uses NSQ 1.3.0 with mem-queue-size 3000, sync-every 2500 and sync-timeout 2s. Ten PUB-confirmed messages are verified in channel memory with no backend, in-flight or deferred messages. SIGKILL exits 137; restart retains the same volume and durable topic/channel, but all ten messages are absent. Three consecutive observations check required fields rather than treating absent fields as zero. This passing characterization proves a limitation, not crash recovery. Graceful shutdown independently preserves ten original identities and payloads.

The SDK cannot discover this loss by scanning pending rows after it has marked publication confirmed. Before each service rollout, the flow needs a demonstrated reconciliation/receipt mechanism adequate for its fault model, or a separately decided broker change. IAM's existing version reconciliation has a direct test. QS flow-specific completeness remains an integration gate. Do not reset all published rows or resend unknown model calls.

**IAM requires exclusive Relay handoff.** Additive claim fields permit fenced writes by the new prototype, but the unchanged old writer can still overwrite the row by event ID. Stop/drain and exclusive forward/rollback handoff, or an independently verified compatible-writer upgrade, is mandatory. The prototype is not production migration SQL.

**QS contains a real business behavior fix.** Concurrent submissions previously could both read pending and emit different evaluation.requested IDs. The candidate updates only a pending, nondeleted Assessment inside the original transaction, then stages its event; one winner succeeds, the loser returns conflict. The cache wrapper forwards this capability and unsupported repositories fail closed. Event insertion failure rolls back the state transition. Cache invalidation timing remains the existing before-outer-commit behavior and is not claimed repaired here.

## Evidence and its limits

Local SDK check/lint, tagged integration vet, script syntax and the full isolated integration suite passed for the crash characterization. The integration executable is not race-instrumented; standard race tests are separate. The prior pushed SDK head 76dec16 and current IAM/QS heads passed their configured CI checks; refreshed SDK CI must be recorded after this package is pushed.

Service proofs use real databases and selected original code paths. QS FIN-loss proof includes the original Worker handler, loopback gRPC and real MySQL intake, but uses fixture AnswerSheet loading/model validation and direct go-nsq settlement. It does not prove the complete production dispatcher, authentication or original producer. Runtime-checkpoint retry authorization is a separate real-SQL proof, not model-call safety. IAM test channels are durable and do not establish production ephemeral-channel lifecycle acceptance.

Required production facts are not inferred from green CI: load thresholds, deployed versions, full-flow coverage, exclusive cutover/rollback, historical backlog, retention and real business behavior remain M3–M6 gates. Redis-index recovery belongs to QS integration; Python and AI frozen/unknown-result semantics remain M7 under the original task owner.

## Next gates

1. Review the API and the host CAS behavior change; reconcile every M2 task with the acceptance index. Record approval scope explicitly before merging these drafts.
2. M3: build the IAM historical adapter and compatible migration; rehearse backlog mapping, exclusive handoff and rollback in isolation before requesting production authorization.
3. M4: implement QS historical adapters and complete producer-to-consumer offline flow evidence, compensation and governance semantics. M5 production cutover follows IAM acceptance and per-flow thresholds.
4. Keep NSQ/RabbitMQ evaluation separate. A broker replacement cannot remove the requirement for a local transactional intent.
