# AI MQ SDK candidate and acceptance boundary

Candidate versions: Python `0.2.0a1` (`python/v0.2.0a1`), additive Go
`v0.3.0-mq-ai.1`. Neither tag is created by this change. Existing Go public
interfaces, broker-confirmed Relay semantics, contracts and golden fixtures stay fixed.

Added capabilities:

- Optional pinned official pynsq/Tornado on the existing asyncio loop; explicit
  startup/drain/stop, owned socket/timer tracking and host commit before FIN.
- Revision 2 codec and failure envelope compatibility, strict workload-bound JOSE
  using jwcrypto and go-jose, local trust maps and final wire preservation.
- Borrowed-transaction MySQL adapters for separate broker publishing and durable
  business confirmation, ordered aggregates and final-ack rearming.
- Normal and worst-failure NSQ size checks for the fixed 262144-byte ceiling.

Local acceptance on 2026-10-03:

| Layer | Evidence | Limit |
| --- | --- | --- |
| Python 3.11 / MySQL 8.0.36 / NSQ 1.3.0 | 47 cases, zero failed/error/skipped | Isolated SDK suite, not host business acceptance |
| Python 3.13 / MySQL 8.4 / NSQ 1.3.0 | 47 cases, zero failed/error/skipped | Same scope |
| Go 1.25.12 | `make check`: format, vet, all unit and race tests | Does not run or sign off M0–M6 business acceptance |
| Go 1.25.9 declared minimum | Added protected and MySQL packages compile/test | Host toolchain proof remains separate |
| NSQ crash loss | PUB OK then owned-broker SIGKILL; empty restarted Broker; original MySQL wire retained and republished | Never uses shared/production NSQ |
| Lost business receipt | Receiver commits; receipt lost; receiver restart/replay deduplicates original ID and one effect | Host production samples remain open |

Python core remains usable without the optional extra. Full acceptance requires
real disposable dependencies and nonempty reports with no skips. The workflow also
builds and installs the wheel away from source and binds asset hashes to GITHUB_SHA.
CI, formal release, final host dependencies/images, mTLS body retrieval, host seams,
historical migration and production family-by-family reconciliation are separate gates.

Host obligations: authenticate before business identity/reference access; persist logical
failure budgets across re-PUB; preserve frozen execution/unknown-call rules; own schema,
transactions, resource shutdown, body retention and MQ-compatible rollback. The SDK does
not grant model retry permission, install host schema or start another scheduler.
