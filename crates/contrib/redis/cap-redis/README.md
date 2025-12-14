# cap-redis

`cap-redis` provides the shared Redis plumbing used by Lattice’s queue bridge and Redis-backed capabilities. It centralises configuration, connection pooling, and key namespacing so downstream crates can focus on higher-level semantics.

## Surface
- `RedisConfig` with namespace-aware helper methods.
- `RedisConnectionFactory` dispensing multiplexed async connections guarded by permits.
- Lightweight key utilities intended for cache, dedupe, and queue crates.

## Next steps
- Add tracing/metrics hooks for connection acquisition.
- Expose async helpers for pipelined command execution patterns.
- Integrate with capability crates (`cap-cache-redis`, `cap-dedupe-redis`) and the queue bridge once they wire in Redis commands.

## Depends on
- Uses `redis` crate for async connections.
- Consumed by optional Redis adapters living under `crates/contrib/redis/`.
