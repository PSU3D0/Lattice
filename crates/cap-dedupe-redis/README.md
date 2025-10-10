# cap-dedupe-redis

`cap-dedupe-redis` supplies the Redis-backed dedupe store required for exactly-once delivery claims. It handles TTL management, collision handling, and instrumentation for idempotency proofs.

## Surface
- `DedupeStore` implementation with `put_if_absent`, `forget`, and metrics hooks.
- Configuration helpers for namespace scoping and TTL defaults.
- Error taxonomy aligning with IDEM* diagnostics.

## Next steps
- Implement Redis commands with async pipelines and reliability instrumentation.
- Write integration tests that exercise duplicate bursts and TTL expiry behaviour.
- Wire into `host-queue-redis` and validator checks in `kernel-plan`.

## Depends on
- Capability traits from `capabilities`.
- Redis connection management shared with `host-queue-redis`.
