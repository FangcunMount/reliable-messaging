# Contributing

Changes are reviewed by @yshujie. Use small `codex/` branches and explain behavior, compatibility, verification and rollback impact.

Run `make check` and `make lint`. Do not commit secrets, generated binaries or local database data. Reference vectors are synthetic; never export production messages into tests.

Pre-1.0 APIs are provisional. No release tag is created until its milestone evidence is complete. v0.1.0 requires the IAM path; v1.0 requires both Go services, upgrade and rollback verification.
