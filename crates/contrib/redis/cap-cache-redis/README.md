# cap-cache-redis

`cap-cache-redis` offers a Redis-based cache capability to support Strict/Stable memoization, cache eviction policies, and policy-driven invalidation triggers.

## Surface
- `Cache` capability implementation with deterministic key encoding.
- TTL/invalidation utilities compatible with RFC §5.2 cache semantics.
- Telemetry for hit/miss rates and cache poisoning detection.

## Next steps
- Layer canonical CBOR+BLAKE3 key derivation helpers on top of the current hex encoding.
- Add tests covering invalidation triggers, TTL-based pruning, and Redis eviction policies.
- Integrate with `kernel-exec` cache hooks and policy enforcement from `policy-engine`.

## Depends on
- `capabilities` trait definitions and idempotency helpers from `dag-core`.
- Shared Redis client plumbing from `cap-redis`.
- Shared duplicate detection logic with `cap-dedupe-redis` for key hashing.
