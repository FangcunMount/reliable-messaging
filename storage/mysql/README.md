# MySQL time and transaction boundaries

The host owns the original transaction, pool, connection location and session timezone. `Bind` and `BindGORM` never open another transaction, change `time_zone`, or commit on the host's behalf.

Standard `rm_outbox` scheduling columns (`created_at`, `next_attempt_at`, `lease_until`, `transport_confirmed_at`) contain UTC clock digits in DATETIME(6). This is an explicit storage convention: MySQL DATETIME does not carry an offset. Applications and operations reports can display these instants in UTC+8. Do not apply the connection location to these raw clock digits when displaying or migrating them.

The adapter encodes UTC scheduling values as date strings, and reads the database clock as an explicitly formatted UTC string. This avoids go-sql-driver/mysql converting `time.Time` arguments into the host connection's `loc`, or interpreting `UTC_TIMESTAMP()` as a different instant on read. `Claim.LeaseUntil` represents the actual UTC instant. Retry/confirmation and fencing use the same database UTC clock. No business message bytes or immutable `occurred_at` values are re-encoded by this correction.

`v0.1.0-m2.1` passed a `loc=UTC` baseline but its standard MySQL Appender can persist an eight-hour-shifted due time on a `loc=UTC+8` host transaction. Calling `due.UTC()` alone does not prevent the driver's subsequent conversion. The M3 correction does not change schema or public APIs, and preserves the UTC baseline. Already persisted rows with a different clock convention need an explicit host audit; this fix does not rewrite history or authorize automatic replay.

The real integration regression varies driver location (UTC/UTC+8), SQL session timezone (+00:00/+08:00), and parameter interpolation (off/on). All eight combinations must preserve persisted due instants, return correct absolute leases, permit retry/confirmation from another pool, and avoid claiming delayed messages early. The pre-fix run fails the four UTC+8 cases; the corrected adapter passes all eight. This does not establish compatibility with arbitrary historical service tables or full IAM cutover acceptance.
