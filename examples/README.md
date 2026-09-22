# M1 host integration references

`make integration` builds and runs `transactional-publisher` inside the disposable MySQL container, then runs `host-lifecycle`. No host DB address is used.

- Transaction example: accepts a typed `*sql.Tx`, rejects nil, writes business and intent with the same transaction, and checks commit/rollback on real MySQL. Host owns setup, transaction and DB close. It does not publish to MQ inside the transaction.
- Lifecycle example: construction is inert; the host explicitly starts work, cancels admission, drains already admitted work, then may close owned resources.

Both are executable integration sketches with private local types, **not** SDK interfaces or a reliable Relay. The lifecycle sketch has no lease renewal, bounded transport timeout or durable recovery. M2 must replace the sketches with proven SDK adapters without changing the ownership boundaries. M1 does not freeze a public API or claim GORM/Mongo bridges have been implemented.
