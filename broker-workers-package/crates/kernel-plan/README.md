# kernel-plan

`kernel-plan` validates Flow IR and lowers it into executable plans tuned for each host profile. It enforces topology, effects/determinism lattice rules, idempotency coverage, and produces the scheduling metadata consumed by executors.

## Surface
- Validation API (`validate`) with structured diagnostics.
- Lowering API (`lower`) that emits profile-specific execution plans.
- Shared diagnostics and rule registries referenced by CLI and registry tooling.

## Next steps
- Implement validators enumerated in RFC §5.4 (DAG200+, IDEM*, CACHE*, etc.).
- Define the execution-plan structures and lowering rules per profile (Web, Queue, Temporal, WASM).
- Add property-based tests for cycle detection, partition semantics, and idempotency guards.

## Depends on
- Requires `dag-core` IR definitions and error-code catalog.
- Should precede `kernel-exec` and host adapters so they can consume stable lowering output.
