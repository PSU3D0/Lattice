# kernel-exec

`kernel-exec` is the in-process runtime responsible for executing lowered plans. It manages scheduling, bounded channels, backpressure, cancellations, checkpoints, and capability injection for inline execution and host adapters.

## Surface
- Executor entry points to run, resume, and inspect workflow instances.
- Scheduler primitives for partitions, rate limits, timeouts, and spill-to-blob.
- Metrics and tracing hooks shared by hosts and observability exporters.

## Next steps
- Implement the async scheduler and bounded-channel semantics described in RFC §6.5.
- Integrate cancellation propagation, spill/backpressure policies, and instrumentation.
- Add unit/integration tests exercising p95 latency targets and backpressure behaviour.

## Depends on
- Requires validated plans from `kernel-plan` and capability traits from `capabilities`.
- Host crates (`host-web-axum`, `host-queue-redis`, etc.) will layer on top once this runtime is stable.
