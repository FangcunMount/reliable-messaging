# M7 independent Python release review

This is a review candidate, not a published Python version or Go milestone acceptance.
Source baseline: Go repository 5323413ddb191a17fa2a43a261b01c0808c34292.
Distribution/import: fangcun-reliable-messaging / reliable_messaging, 0.1.0a1.

## Scope and evidence

- All additions are Python package/tests and a separate Python workflow. Existing Go
  interfaces, wire encoders, golden fixtures and M0–M6 checks are unchanged.
- Existing ten identity golden vectors plus an M7 oracle calling existing Go public
  constructors cover hash, IDs, UTF-8 byte bounds, uint64 JSON values, defaults/null,
  original timestamp offsets/fractions and outcome numeric values. Host wire validation
  is not inferred from generic Message validity.
- 20 local tests passed, no skips: async settlement/lifecycle plus real MySQL original
  transaction, commit/rollback, cancellation, replacement/savepoint rejection, borrowed
  engine usability, unchanged JSON, duplicate acknowledgement, clocks and subprocess
  SIGKILL/restart/notification loss.
- Local storage runs: MySQL 8.0.44 with SQLAlchemy 2.0.52; a second fully separate native
  MySQL 9.3 with SQLAlchemy 2.0.54. Native 9.3 is supplemental, not target-version proof.
  The independent workflow tests Python 3.11/MySQL 8.0.36 and Python 3.13/MySQL 8.4;
  its terminal results must be checked before treating those combinations as verified.
- Ruff format/lint and mypy passed. Independent wheel/sdist built locally. Local wheel
  hash: d18fc1d4227383c03dafae6e1d5d31d0f6794b032094053d181c54e291a63aff.
  A release must bind its own exact CI artifact hashes, not reuse an unrelated build.

## Constraints and cutover

Only the pending/delivered existing-table adapter is supported. No Python NSQ,
leased-store fencing, new storage service, schema migration, model execution or
business retry authorization. Constructors own no network/pools/tasks.

qs-ai keeps its gRPC durable receipt and single-process supervisor. It replaces the
receiver-success/failure settlement algorithm and pending/confirm/retry SQL through
this package, preserving StateEvent construction, unique session/version staging,
timestamps, host commits and diagnostics. It does not run a second scheduler.

Before publication: review precise source/artifacts, terminal Python CI, version
availability and release destination; explicitly authorize formal Python publication.
Before production adoption: replace a temporary immutable Git source pin with the
approved published wheel/index version; rebuild the normal qs-ai image from fixed
source and lockfile, verify no Git tooling is required in the runtime build, and
review signed artifact/config/rollback evidence. No Git checkout paths enter runtime.

Retain the previous qs-ai image/config. Stop result admission and drain the old
single-process service before starting the new one. The database schema and gRPC
identity stay unchanged, so the old image can read undelivered rows if rolled back.
Do not discard pending rows or change accepted tasks, model bindings, candidate mode,
capacity or recovery authorizations during cutover. Production acceptance and original
qs-ai execution-owner approval remain separate from local SDK proof.
