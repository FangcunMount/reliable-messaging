# M7 independent Python release review

## Current publication record

The project initiator approved publication, and the independent GitHub prerelease
[`python/v0.1.0a1`](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.1.0a1)
was published from `fea92389967076e3529289e80b1d96abef76dc73`.
Final Python CI run `37092771662` passed both required combinations with 22 tests each
and no failures, errors or skips. Published assets were downloaded and verified:

- Wheel SHA256: `5699421a213a6824cb39f13dc9b10c562693137670a071077ba5e50e687fec9e`.
- Source archive SHA256: `9bac3753405ad0c3eee3c0b5e938941c6b8aed2445a5b4268f5ff68d9e8b635b`.

The tag and assets must not be replaced. Publication is not Go milestone acceptance.
The following sections preserve the original pre-publication review and constraints.

## Original review

This was a review candidate, not a published Python version or Go milestone acceptance.
Source baseline: Go repository 5323413ddb191a17fa2a43a261b01c0808c34292.
Distribution/import: fangcun-reliable-messaging / reliable_messaging, 0.1.0a1.
Planned destination: GitHub Release, prerelease tag `python/v0.1.0a1`.
Do not create or replace the tag or upload formal assets before the project
initiator approves the exact source, terminal CI and asset checksums.

## Scope and evidence

- All additions are Python package/tests and a separate Python workflow. Existing Go
  interfaces, wire encoders, golden fixtures and M0–M6 checks are unchanged.
- Existing ten identity golden vectors plus an M7 oracle calling existing Go public
  constructors cover hash, IDs, UTF-8 byte bounds, uint64 JSON values, defaults/null,
  original timestamp offsets/fractions and outcome numeric values. Host wire validation
  is not inferred from generic Message validity.
- 22 tests cover async settlement/lifecycle plus real MySQL original
  transaction, commit/rollback, cancellation, replacement/savepoint rejection, borrowed
  engine usability, unchanged JSON, duplicate acknowledgement, clocks and subprocess
  SIGKILL/restart/notification loss.
- The added final gate rejects actual driver AUTOCOMMIT even when the SQLAlchemy
  transaction object claims to be active. The supported original binding is MySQL
  asyncmy; unverified driver modes fail closed. Final-source CI must include this gate.
- Local storage runs: MySQL 8.0.44 with SQLAlchemy 2.0.52; a second fully separate native
  MySQL 9.3 with SQLAlchemy 2.0.54. Native 9.3 is supplemental, not target-version proof.
  The independent workflow tests Python 3.11/MySQL 8.0.36 and Python 3.13/MySQL 8.4;
  both combinations passed at 82ce922 (run 37090106558). That is previous-source
  evidence; subsequent candidate changes need their own terminal CI record.
- Ruff format/lint and mypy passed. Independent wheel/sdist built locally; exact
  candidate source/artifact hashes belong in the M7 evidence record. A release must
  bind its own exact CI artifact hashes, not reuse an unrelated build.
- The first target-version CI exposed missing cryptography for cold caching_sha2
  authentication; the test dependency is now explicit. Failed logs are retained.
  Superseded 20/21-test CI runs are not acceptance of the final 22-test source.

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
review exact artifact/config/rollback evidence. No artifact signature is claimed by
this candidate; source identity, CI identity and SHA256 are recorded separately.
No Git checkout paths enter runtime.

The wheel must install in a separate environment, expose version 0.1.0a1 and include
py.typed. CI reports must contain actual test cases, with zero failures, errors or
skips; missing dependencies fail the required integration. Both matrix artifacts
must bind the same final source and match their wheel/source archive checksums.

The published URL is fixed to the tagged GitHub Release asset, never latest or a
branch URL. Keep its hash in the host lockfile. GitHub assets can technically be
replaced: the release policy forbids replacement, and a changed hash must fail
installation. This prerelease does not add a license or publish to PyPI.

Retain the previous qs-ai image/config. Stop result admission and drain the old
single-process service before starting the new one. The database schema and gRPC
identity stay unchanged, so the old image can read undelivered rows if rolled back.
Do not discard pending rows or change accepted tasks, model bindings, candidate mode,
capacity or recovery authorizations during cutover. Production acceptance and original
qs-ai execution-owner approval remain separate from local SDK proof.

qs-ai currently enables automatic deployment on main. Treat its main merge as part
of the production approval, not a preparatory code-only operation. Do not change
deployment switches to work around this gate. Original qs-ai/qs-server owners must
provide their approved source/build and execution-recovery evidence; Python CI does
not run or sign off Go M0-M6 acceptance.
