# Isolated integration environment

Run `make integration` with Go 1.25.12, Docker, Compose v2+ supporting `up --wait` and Python 3. Missing tools or unavailable Docker fail; there is no skip path. A remote Docker endpoint or DOCKER_HOST override is rejected.

Every invocation creates a random Compose project with an internal network, no published ports, no bind mounts, and disposable data: MySQL/Mongo use tmpfs; NSQ uses a per-project named volume so a real broker stop/start can preserve its data. Cleanup removes only that invocation's resources, on success, failure or an ordinary interrupt. SIGKILL/host crash may leave labelled Compose resources: inspect their project ownership before manual cleanup; never prune all Docker data.

Images are pinned by manifest digest: MySQL 8.0.44, MongoDB 7.0.37 and NSQ 1.3.0, using the M0 baseline. This is not a supported-production-version promise. The MySQL empty password is restricted to the disposable internal network; never use this compose file for deployment.

Current probes:

- MySQL real commit and rollback of business/outbox rows.
- Mongo replica-set initialization, primary readiness and real two-collection commit/abort.
- NSQ publish accepted and message present in the intended channel.
- SDK transaction/fencing, Relay process failure, database write failure, publication-confirmation loss and late-result tests.
- Real graceful NSQ stop/start over the same disposable volume: 10 preconfirmed messages retain original identities/bytes.
- A compiled Go SQL example executing in the isolated MySQL container, and a host lifecycle reference running locally.

The SDK tests distinguish real adapters from explicit transport/consumer doubles; detailed coverage is in docs/m2-evidence.md. Service-specific original-consumer proofs live in IAM/qs-server drafts. No dependency address is read from host application configuration.

The broker restart fixture uses NSQ 1.3.0 with mem-queue-size=3000, sync-every=2500 and sync-timeout=2s, matching observed M0 settings. The script requires a clean zero exit before starting the broker again, then consumes every seeded body. This proves graceful-stop flushing/recovery, **not** SIGKILL safety, OS power loss, lost disk/node, replication or fsync-per-PUB durability. The named volume is removed by the same project-scoped cleanup trap; it is never a production volume.

The separate SIGKILL characterization uses its own topic and per-batch random application identities. Before killing the broker, three stats samples require all 10 confirmed messages in channel memory and zero backend/in-flight/deferred entries. The harness requires exit 137, starts the same volume, and observes the durable topic/channel present but those queues empty. `OBSERVED LIMIT` means the expected loss boundary was reproduced, **not** crash recovery passed. The first shared-topic variant did not satisfy the empty-queue assertion and is not used as evidence; separate topics prevent prior graceful-restart backlog from contaminating the experiment.
