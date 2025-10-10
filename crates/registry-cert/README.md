# registry-cert

`registry-cert` runs the certification harnesses that gate publishing connectors and flows. It coordinates determinism replay, idempotency injection, contract tests, and policy evidence collection.

## Surface
- Command-friendly APIs for invoking certification suites.
- Evidence bundling utilities consumed by `registry-client`.
- Hooks for additional harnesses (Temporal, cache, security) as they come online.

## Next steps
- Implement determinism + idempotency harness wiring (leveraging `testing-harness-idem`).
- Add fixture-driven contract testing for HTTP connectors.
- Produce evidence bundles aligning with RFC §10 and integrate with CLI commands.

## Depends on
- Test harness crates (starting with `testing-harness-idem`).
- Flow IR/types from `dag-core` and registry interactions from `registry-client`.
