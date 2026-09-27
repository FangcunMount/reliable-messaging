# Isolated integration environment

Run `make integration` with Go 1.25.12, Docker, Compose v2+ supporting `up --wait` and Python 3. Missing tools or unavailable Docker fail; there is no skip path. A remote Docker endpoint or DOCKER_HOST override is rejected.

Run `make subscription-integration` for the focused M6-04B NSQ consumer proofs. It uses the same isolated Compose manifest but starts only MySQL (as a network-isolated test runner) and NSQ, compiles the integration test binary, and runs the lost-confirmation binding and direct-address Subscriber lifecycle cases. Their failure handler is a controlled test callback rather than a durable database recorder; lookupd topology and service-level dead-letter audit remain separate acceptance gates.

Every invocation creates a random Compose project with an internal network, no published ports, no bind mounts, and disposable data: MySQL/Mongo use tmpfs; NSQ uses a per-project named volume so a real broker stop/start can preserve its data. Cleanup removes only that invocation's resources, on success, failure or an ordinary interrupt. SIGKILL/host crash may leave labelled Compose resources: inspect their project ownership before manual cleanup; never prune all Docker data.

Images are pinned by manifest digest: MySQL 8.0.44, MongoDB 7.0.37 and NSQ 1.3.0, using the M0 baseline. This is not a supported-production-version promise. The MySQL empty password is restricted to the disposable internal network; never use this compose file for deployment.

Current probes:

- MySQL real commit and rollback of business/outbox rows.
- Mongo replica-set initialization, primary readiness and real two-collection commit/abort.
- NSQ publish accepted and message present in the intended channel.
- SDK transaction/fencing, Relay process failure, database write failure, publication-confirmation loss and late-result tests.
- Actual SDK `v0.1.0` and `v0.2.1` binaries built from verified release tags: old transaction and pending intent, new Store rejection before additive DDL, old Appender after DDL, new Store draining old identities, and old Store draining an ordinary new pending intent. This is a sequential, basic-state handoff on disposable MySQL; it does not prove concurrent old/new Relays, diagnostic-field fidelity after downgrade or a production rollback window.
- The same two released binaries also run a sequential Mongo replica-set handoff: v0.1.0 writes original transaction facts, v0.2.1 indexes and drains old pending documents, then v0.1.0 drains a new ordinary pending document. Three old documents still lack `created_at`, which is an explicit host status-reader compatibility gap. This is not a production Mongo migration or mixed-version concurrency proof.
- Both storage handoffs also cross v0.1.0 → v0.2.1 → v0.1.0 → v0.2.1 on one original retry identity. The old Store does not increment the new `failure_count` column/field; after three Retry transitions and one Quarantine, the persisted count is only two. This deliberately characterizes an unsafe diagnostic downgrade, not a passing compatibility guarantee. Never downgrade a retrying Relay based only on a successful ordinary pending-message handoff; inventory unfinished state and keep a compatible execution owner.
- Each released Store also leaves one real publishing lease for the other release to recover, in both directions and both databases. The next release must not claim before expiry, must reclaim the same original identity and bytes after expiry with a new token/version and `attempt_count=2`, must reject the prior claim's write, and must Confirm with `failure_count=0`. This is a sequential process-exit lease test, not simultaneous mixed-version Relay ownership or a broker confirmation-lost test.
- Real graceful NSQ stop/start over the same disposable volume: 10 preconfirmed messages retain original identities/bytes.
- A compiled Go SQL example executing in the isolated MySQL container, and a host lifecycle reference running locally.

The SDK tests distinguish real adapters from explicit transport/consumer doubles; detailed coverage is in docs/m2-evidence.md. Service-specific original-consumer proofs live in IAM/qs-server drafts. No dependency address is read from host application configuration.

The broker restart fixture uses NSQ 1.3.0 with mem-queue-size=3000, sync-every=2500 and sync-timeout=2s, matching observed M0 settings. The script requires a clean zero exit before starting the broker again, then consumes every seeded body. This proves graceful-stop flushing/recovery, **not** SIGKILL safety, OS power loss, lost disk/node, replication or fsync-per-PUB durability. The named volume is removed by the same project-scoped cleanup trap; it is never a production volume.

The separate SIGKILL characterization uses its own topic and per-batch random application identities. Before killing the broker, three stats samples require all 10 confirmed messages in channel memory and zero backend/in-flight/deferred entries. The harness requires exit 137, starts the same volume, and observes the durable topic/channel present but those queues empty. `OBSERVED LIMIT` means the expected loss boundary was reproduced, **not** crash recovery passed. The first shared-topic variant did not satisfy the empty-queue assertion and is not used as evidence; separate topics prevent prior graceful-restart backlog from contaminating the experiment.
