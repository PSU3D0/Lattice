# studio-backend

`studio-backend` is the service layer that powers agent workflows and UI automation. It exposes APIs to compile flows, run tests, manage HITL checkpoints, and publish artifacts, all building on the CLI primitives.

## Surface
- HTTP/JSON API (Axum) wrapping compile/run/certify/publish operations.
- Session + auth scaffolding for future multi-tenant support.
- Telemetry endpoints surfacing policy evidence and run metrics.

## Next steps
- Stand up minimal Axum app calling into `kernel-plan`, `kernel-exec`, and `registry-client`.
- Add integration tests for compile/run endpoints once the underlying crates are functional.
- Plan authentication/authorization hooks for later phases, even if stubbed now.

## Depends on
- Downstream functionality from `dag-core`, `kernel-plan`, `kernel-exec`, and `registry-client`.
- Policy evidence generation in `policy-engine` and `registry-cert`.
