# Pinned RocketMQ Go runtime

This internal snapshot derives from Apache RocketMQ Go client v5.1.4 under the
included Apache License 2.0. Original file headers are retained. `UPSTREAM.json`
records the original runtime file hashes; `rpc-options.patch` records our changes.
Credentials, generated protocol and utility packages remain pinned module imports.
This is not an Apache release or a public SDK API.

The upstream client constructs its RPC manager without passing the configured
connection/RPC options. Consequently `WithConnOptions(WithTLSConfig(...))` does
not configure the actual RPC connections, whose default skips TLS verification.
The same omission prevents connection interceptors from observing real RPCs.

The local patch carries per-client connection and RPC options into that client's
manager and into each real RPC connection. No process-wide factory is replaced.
TLS trust, hostname rejection and interceptor execution are covered by an actual
TLS gRPC server test. Unary RPC/dial deadlines release their cancellation
resources; telemetry keeps its deadline until stream failure or close. The
stream regression test ensures it is not canceled when its factory returns.
Broker acceptance separately covers both normal and
transactional messages. Keep this snapshot private; remove it when a reviewed,
fixed upstream release can pass the same gates.

Startup failure shutdown broadcasts channel closure and closes its RPC manager;
it must not wait for an absent unbuffered channel reader. The TLS rejection test
also checks that startup returns within its bounded failure budget. The additional `transaction-runtime.patch` carries caller contexts through both
connection establishment and Commit/Rollback, bounds initial-settings startup,
provides pre-goroutine recovery admission, and joins owned client/session
workers. Interruptible initial delays and closed-manager connection rejection
prevent background resources from being recreated during shutdown.

Local real-TLS tests cover commit/rollback cancellation, cold TLS connection
budgets, missing initial settings, 1000 recovery commands behind one admission
slot, and scheduled-worker shutdown. Adapter queue/drain and real Broker
transaction proofs are separate gates; these tests do not replace them.
