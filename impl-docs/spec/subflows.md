Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2026-01-29

# Subflows (0.1.x)

Subflows are reusable flow bundles with **typed entrypoints** and **typed outputs**. A subflow can be
invoked as a node in another flow while preserving compile-time validation.

## Goals

- Enable composition and reuse of flow logic.
- Preserve compile-time linking and validation.
- Treat subflows as compile-time linked Rust dependencies.

## Non-goals

- Dynamic subflow discovery at runtime.
- Cross-host execution or remote subflow invocation (future).

## Subflow Contract

Each subflow exposes one or more entrypoints with explicit schemas and aggregated metadata.

```
SubflowEntrypoint {
  name: "default",
  input_schema: JsonSchema,
  output_schema: JsonSchema,
}

SubflowDescriptor {
  id: "subflow-id",
  version: "1.2.0",
  entrypoints: [SubflowEntrypoint],
  effects: Effects,
  determinism: Determinism,
  effect_hints: ["resource::http::write", ...],
  durability: DurabilityProfile,
}
```

Entrypoint schemas are derived from Rust types when authored in code. The descriptor is computed
at build time and bundled with the FlowBundle.

## Invocation Model

Subflows are represented as **nodes** in a parent flow:

- `NodeKind::Subflow` for the node kind.
- Node identifier: `subflow::<subflow_id>::<entrypoint>`.
- Node input/output schemas match the subflow entrypoint.

During compilation, the parent crate links the subflow crate directly. The subflow node handler is
generated at build time, which ensures missing subflows are compile errors.

Canonical authoring pattern:

```
subflow!(crate::flow_mod::entrypoint)
```

There is no runtime `flows import` mechanism in 0.1.x.

## Execution Semantics

- Subflow executes **in-process** on the same host runtime.
- Subflow inherits the same `ResourceBag` unless explicitly restricted by the host.
- Subflow execution is a **node boundary**: its aggregated metadata is used for validation and
  preflight checks.

### Aggregated Metadata

Subflow nodes carry aggregated metadata derived from the subflow graph:

- `effects`: worst-case across nodes (e.g. if any node is effectful, subflow is effectful).
- `determinism`: worst-case across nodes (e.g. nondeterministic if any node is).
- `effect_hints`: union of all node effect hints.
- `durability.checkpointable`: true only if all nodes are checkpointable.
- `durability.replayable`: true only if all streaming nodes are replayable.
- `durability.halts`: true if any node halts (parent node becomes a halt boundary).

This aggregation is performed at compile time and stored in the SubflowDescriptor.

## Versioning

Subflows should be versioned independently (e.g., `subflow_id@1.2.0`).
Parent flows should reference a specific version to ensure stability.

## Validation Rules

- Subflow entrypoint schemas must match the parent node schemas.
- Node-to-subflow type checking uses the Level-2 rule `Out: Into<In>` (matching EdgeIR).
- Cycles through subflows are disallowed (no recursion) in 0.1.
- If parent flow requires `durability=strong`, subflow must be fully checkpointable.
- If subflow contains a halt node, the parent node is treated as a halt boundary.
- Effect hints and idempotency rules apply to subflow nodes like any other node.

## Packaging

Subflows are compile-time linked Rust crates. A FlowBundle may include a SubflowCatalog that maps
subflow IDs to their descriptors, but all subflow code is linked into the parent binary for 0.1.x.

## IR Representation

Subflow invocations are normal NodeIR entries with:

- `kind: subflow`
- `identifier: subflow::<subflow_id>::<entrypoint>`
- `input_schema` / `output_schema` matching the subflow entrypoint
- `durability` and `effects` populated from the SubflowDescriptor

The actual subflow graph is not embedded in the parent Flow IR; it is referenced via the catalog.
