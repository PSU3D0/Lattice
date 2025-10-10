# dag-core

`dag-core` defines the foundational types, traits, and Flow IR structures that every other LatticeFlow crate consumes. It covers effect/determinism enums, node/trigger contracts, capability references, and the serialisable IR that powers validation, planning, and registry export.

## Surface
- Core trait definitions for `Node`, `Trigger`, `Capability`, and execution contexts.
- Flow IR data structures plus serde/schemars integration for export tooling.
- Shared error and result types used across hosts, planners, and connectors.

## Next steps
- Flesh out the type system according to RFC §4–§5, including canonical idempotency/caching specs.
- Add serde/schemars implementations and unit tests for IR round-tripping.
- Wire in lint helpers that downstream crates can reuse for effect/determinism validation.

## Depends on
- No prior crates are required, but this crate should land before `dag-macros`, `kernel-plan`, and `capabilities` implementations begin.
