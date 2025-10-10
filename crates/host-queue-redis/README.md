# host-queue-redis

`host-queue-redis` implements the queue profile using Redis as the durable transport. It supports visibility timeouts, at-least-once delivery, dedupe integration, and fairness scheduling for sharded partitions.

## Surface
- Enqueue/dequeue APIs integrated with `kernel-exec` run loops.
- Worker management (spawning, heartbeats, visibility extensions).
- Instrumentation for queue depth, retries, and spill events.

## Next steps
- Implement worker loop and error handling per RFC §6.4.
- Add integration tests with `cap-dedupe-redis` and `cap-cache-redis` to validate exactly-once edges.
- Document operational tuning (visibility timeout, shard concurrency) for CLI use.

## Depends on
- Requires `kernel-exec`, `capabilities`, and Redis-backed capability crates.
