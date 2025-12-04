# bridge-queue-redis

`bridge-queue-redis` implements the queue profile bridge using Redis as the durable transport. It feeds canonical invocations into the shared `host-inproc` runtime while handling visibility timeouts, at-least-once delivery, dedupe integration, and fairness scheduling for sharded partitions.

## Surface
- `QueueBridge::new` accepts a `HostRuntime` and `QueueBridgeConfig` (Redis namespace, blocking timeout, idle backoff).
- `enqueue` serialises invocations into Redis lists with namespaced keys.
- `spawn_workers` runs Tokio tasks that `BLPOP`, reconstruct invocations, and delegate execution to `host-inproc`.

## Next steps
- Extend the lease reaper to operate against batch workers and emit backpressure when the ready list grows beyond policy thresholds.
- Integrate queue-level configuration into the validator so Delivery::ExactlyOnce edges assert dedupe bindings at compile time.
- Add integration tests exercising enqueue → worker execution using dockerised Redis fixtures and synthetic dedupe failures.

## Depends on
- `host-inproc` runtime for execution.
- Shared Redis plumbing from `cap-redis`.
