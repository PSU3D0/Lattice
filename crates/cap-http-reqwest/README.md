# cap-http-reqwest

`cap-http-reqwest` offers a Reqwest-backed implementation of the HTTP read/write capabilities, complete with rate limiting, retries, and pinned-resource support for determinism downgrades.

## Surface
- Concrete `HttpRead`/`HttpWrite` capability providers.
- Shared retry/backoff helpers aligned with policy defaults.
- Instrumentation hooks for request budgeting and rate-limit events.

## Next steps
- Implement read/write methods with pinned URL handling and structured errors.
- Add integration tests using `tokio::test` and recorded fixtures.
- Wire provider registration into `capabilities` and the registry evidence harness.

## Depends on
- Requires trait definitions from `capabilities` and scheduler hooks from `kernel-exec`.
- Leverages shared error codes and policy rules defined in `policy-engine` later on.
