# cap-dedupe-redis

`cap-dedupe-redis` supplies the Redis-backed dedupe store required for exactly-once delivery claims. It handles TTL management, collision handling, and instrumentation for idempotency proofs.

## Surface
- `DedupeStore` implementation with `put_if_absent`, `forget`, and metrics hooks.
- Configuration helpers for namespace scoping and TTL defaults.
- Error taxonomy aligning with IDEM* diagnostics.

## Next steps
- Add tracing/metrics instrumentation around Redis calls to feed queue diagnostics.
- Write integration tests that exercise duplicate bursts and TTL expiry behaviour.
- Wire into `bridge-queue-redis` and validator checks in `kernel-plan`.

## Depends on
- Capability traits from `capabilities`.
- Shared Redis client plumbing from `cap-redis`.
- Queue bridge/integration tests in `bridge-queue-redis`.
