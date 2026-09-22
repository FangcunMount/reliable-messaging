# Milestones

M0 analysis accepted after the project initiator confirmed final review ownership for SDK, IAM, qs-server and operations.

M1 accepted by the project initiator and PR #1 squash-merged on 2026-09-22 (main 6069a1a). Local checks and all four PR/push CI jobs passed before approval.

M2 in progress on `codex/m2-mysql-outbox`: provisional immutable Message, original SQL transaction Appender, standard MySQL Outbox and fenced claims. See [M2 evidence](m2-evidence.md). This is not a historical-schema replacement or complete delivery loop. Relay, NSQ adapter, dual-storage/original-UoW proofs, observer integration and fault matrix remain open. APIs remain provisional.

M3–M6 not started. No service uses this SDK yet; no production cutover or broker migration is authorized. Python remains deferred to M7 and the separate qs-ai acceptance baseline.

Reference selection: extract a small core over mature drivers. Watermill default SQL/Forwarder would require old-schema, envelope, due/lease and governance customization; no measured performance advantage is claimed. Revisit if real dual-storage proofs invalidate the chosen contracts.
