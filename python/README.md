# Python minimal SDK (M7 candidate)

Independent distribution `fangcun-reliable-messaging`, import `reliable_messaging`,
candidate version `0.1.0a1`. This version is not a formal release. Python 3.11–3.13,
SQLAlchemy 2.0 async; no dependency on host repositories or the Go runtime.

Supported scope: immutable message identity/fingerprint, durable result acknowledgement
settlement, original SQLAlchemy transaction binding, an adapter over an **existing**
pending/delivered JSON outbox, and optional explicit async start/stop polling.
There is no Python broker, leased Go Store, workflow engine, schema migration or new database.

## Resource and transaction ownership

```python
from reliable_messaging.sqlalchemy import bind

async with host_session.begin():
    await host_session.execute(business_insert)
    await bind(host_session).append(outbox_insert)
```

The host supplies an INSERT (including its original duplicate identity policy).
Binding needs an already active MySQL AsyncSession/AsyncConnection transaction. It also
supports a transaction autobegun by earlier host operations; bind/append itself never
begins one. A binding cannot outlive its original transaction or savepoint. Append
does not begin/commit/rollback/close a transaction or dispose a pool. The host must
roll back on append failure or cancellation. No schema is installed by constructors.
Before writing, the adapter checks the borrowed driver's actual get_autocommit flag;
SQLAlchemy's logical begin alone cannot prove a real transaction. AUTOCOMMIT and
drivers without that verification method are rejected. asyncmy is the tested driver;
other databases/drivers are not claimed as supported by this minimal appender.

`MySQLPendingOutbox` borrows the host Table with event_id/payload/delivered/attempts/
available_at/delivered_at columns. Settlement needs an explicit host transaction;
the host commits. Reads use SQLAlchemy's normal session behavior. Times are the
existing UTC_TIMESTAMP(6) database clock, independently of session time zone.
Host process runtime remains UTC+8; original RFC3339 strings are never normalized.

This adapter is for qs-ai's current single-process, duplicate-safe durable receiver.
It does not claim multi-process fencing, Go storage-schema compatibility or new
retry/quarantine authorization. SQL expressions preserve qs-ai's existing bounded
retry schedule, including MySQL's assignment evaluation order.

## Durable acknowledgement

`deliver_durable(event_id, store, accept)` executes a callback that verifies the
**business receiver's durable receipt for that original event_id**. qs-ai retains its
gRPC protobuf and matching receipt check. The callback returns only after acceptance.
Broker confirmation cannot be passed as this callback. Confirmed/Unknown/Rejected
have the Go numeric values 1/0/2; confirmation layer is separately explicit.

Receiver exceptions requeue the original notification through the host store. Timeout
or lost receipt is Unknown. An explicit DeliveryRejected reports Rejected; the host
still owns eligibility. Cancellation is propagated without confirming or requeueing.
Settlement errors propagate to the host supervisor; durable records remain recoverable.
No task execution, frozen configuration, model call, checkpoint, capacity or business
recovery state is read or changed by this API.

## Lifecycle

```python
from reliable_messaging import PeriodicRelay

relay = PeriodicRelay(deliver_committed_results, poll_seconds=1, shutdown_seconds=5)
await relay.start()
# relay.notify() is a lossy post-commit hint, not a durable message.
await relay.stop()  # drain first; then the host may close its own DB/channel
```

Construction performs no I/O or task creation. Polling after restart and without
notifications still scans durable state. start rejects an already-started relay;
wait/stop surfaces failed scans. stop ends admission, waits for the current attempt,
then cancels on deadline and raises TimeoutError; committed rows are left for recovery.
Callbacks must support cooperative cancellation. No OS signals or resources are owned.

qs-ai already has a single-process supervisor and polling loop. It continues using that
loop and invokes shared settlement/storage primitives; it does not run a second Relay.

## Contracts and validation

Message matches the existing Go message.Input byte limits, identity tuple and
rm-fingerprint-draft-v1 algorithm. Payload bytes remain opaque: no JSON canonicalization,
business validation or timestamp rewriting. Hash equivalence does not imply valid
business input. The existing contracts/fixtures/identity-vectors.json is read directly
by tests; its invalid host envelopes are still valid generic delivery intents.

Tests separate fixtures, async unit semantics and disposable MySQL/process recovery.
Required MySQL tests fail without RM_M7_MYSQL_URL; they never silently skip.
No real model calls, candidate activation, production access or Go acceptance replay.

Run from python/: `PYTHONPATH=src pytest -m 'not integration'`; for real storage,
set a disposable `RM_M7_MYSQL_URL` and run `PYTHONPATH=src pytest -m integration`.
Use `uv build` for independent wheel/sdist. Formal Python publication and qs-ai
production cutover require separate review of fixed artifact hashes, CI and rollback.
