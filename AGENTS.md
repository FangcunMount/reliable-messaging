# Engineering rules

This is an embedded Go SDK, not a service. The project initiator reviews SDK, IAM, qs-server and operations changes.

- Preserve host transaction ownership, original message identity and immutable bytes.
- No dependencies on IAM, qs-server, qs-ai or component-base. Database and broker drivers belong only in their adapters.
- No network, goroutines, schema migration or resource ownership hidden in constructors.
- No business retry authorization, workflow engine or production configuration here.
- Public APIs remain provisional until MySQL and Mongo transaction proofs pass.
- Tests must distinguish reference contracts, adapter tests, real integrations and fault injection. Required integration tests fail if dependencies are missing; never silently skip.
- Run `make check`; use isolated resources for integration. Production writes or cutover require separately authorized concrete plans.
- Keep qs-ai execution/recovery code under its original task owner.
