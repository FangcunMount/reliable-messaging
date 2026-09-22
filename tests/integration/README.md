# Isolated integration environment

Run `make integration` with Go 1.25.12, Docker, Compose v2+ supporting `up --wait` and Python 3. Missing tools or unavailable Docker fail; there is no skip path. A remote Docker endpoint or DOCKER_HOST override is rejected.

Every invocation creates a random Compose project with an internal network, no published ports, no bind mounts, and disposable tmpfs data. Cleanup removes only that invocation's resources, on success, failure or an ordinary interrupt. SIGKILL/host crash may leave labelled Compose resources: inspect their project ownership before manual cleanup; never prune all Docker data.

Images are pinned by manifest digest: MySQL 8.0.44, MongoDB 7.0.37 and NSQ 1.3.0, using the M0 baseline. This is not a supported-production-version promise. The MySQL empty password is restricted to the disposable internal network; never use this compose file for deployment.

Current probes:

- MySQL real commit and rollback of business/outbox rows.
- Mongo replica-set initialization, primary readiness and real two-collection commit/abort.
- NSQ publish accepted and message present in the intended channel.
- A compiled Go SQL example executing in the isolated MySQL container, and a host lifecycle reference running locally.

These prove the test infrastructure is usable. They do **not** exercise the future SDK or prove consumer ACK, disk durability, node-loss recovery, lease fencing or transactional adapters. M2 extends the same isolated environment with adapter and fault tests. No dependency address is read from host application configuration.
