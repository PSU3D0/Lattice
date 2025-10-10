# testing-harness-idem

`testing-harness-idem` provides test utilities for validating idempotency guarantees across flows and connectors. It injects duplicate/reordered deliveries and collects evidence used by certification.

## Surface
- Harness runner API invoked by `registry-cert` and CLI tools.
- Fixtures/helpers for generating duplicate payloads and asserting single side-effects.
- Telemetry hooks feeding policy evidence bundles.

## Next steps
- Implement stream injection helpers and assertion macros per RFC §5.4.
- Add tests covering burst duplicates, late arrivals, and ExactlyOnce edges.
- Expose convenience methods for connector authors to embed in their test suites.

## Depends on
- Flow IR types from `dag-core` and runtime hooks from `kernel-exec` once they emit run receipts.
