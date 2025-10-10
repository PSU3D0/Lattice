# plugin-python

`plugin-python` manages out-of-process Python plugins over gRPC. It provides sandboxing (network, CPU, memory), schema negotiation, and deterministic IO bridging so data-science style nodes can participate safely.

## Surface
- gRPC service definitions and transport harness.
- Sandbox configuration (seccomp/AppArmor placeholders, resource limits).
- Capability adapters exposed to Python clients.

## Next steps
- Define proto contracts and code-generate Rust/Python stubs.
- Implement supervision (process lifecycle, health checks, restarts).
- Add integration tests that execute the S7 data pipeline example end-to-end.

## Depends on
- Core IR/capability definitions from `dag-core` and `capabilities`.
- Shared tooling from `kernel-exec` and `policy-engine` for budget enforcement.
