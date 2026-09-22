# Milestones

M0 analysis accepted after the project initiator confirmed final review ownership for SDK, IAM, qs-server and operations.

M1 awaiting review: private repository, minimal Go tooling, reference fixtures, dependency checks, repeatable isolated MySQL/Mongo/NSQ probes and executable transaction/lifecycle references are implemented. Local check, lint and integration pass; PR CI and final review remain the exit gate. The probes are infrastructure and host-ownership demonstrations, not SDK fault-matrix acceptance.

M2–M6 not started. No service uses this SDK yet; no production cutover or broker migration is authorized by this repository scaffold. Python remains deferred to M7 and the separate qs-ai acceptance baseline.

Reference selection: extract a small core over mature drivers. Watermill default SQL/Forwarder would require old-schema, envelope, due/lease and governance customization; no measured performance advantage is claimed. Revisit if real dual-storage proofs invalidate the chosen contracts.
