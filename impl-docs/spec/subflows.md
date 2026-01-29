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
- Allow subflows to be distributed as crates or bundles.

## Non-goals

- Dynamic subflow discovery at runtime.
- Cross-host execution or remote subflow invocation (future).

## Subflow Contract

Each subflow exposes one or more entrypoints with explicit schemas.

```
SubflowEntrypoint {
  name: "default",
  input_schema: JsonSchema,
  output_schema: JsonSchema,
}
```

Entrypoint schemas are derived from Rust types when authored in code.

## Invocation Model

Subflows are represented as **nodes** in a parent flow:

- Node identifier: `subflow::<subflow_id>::<entrypoint>`
- Node input/output schemas match the subflow entrypoint.

During compilation, the parent bundle must include the referenced subflow bundle in its catalog. The
node handler for the subflow is generated at build time, which ensures missing subflows are compile
errors.

## Execution Semantics

- Subflow executes **in-process** on the same host runtime.
- Subflow inherits the same `ResourceBag` (capabilities) unless explicitly restricted.
- Subflow execution is a **node boundary**: its effects/determinism are summarized for validation.

### Aggregated Effects

Subflow nodes carry aggregated metadata:

- `effectful = true` if any node inside is effectful.
- `checkpointable = false` if any node inside is checkpoint-incompatible.
- `idempotency` requirements bubble up where needed.

This aggregation is performed at compile time when building the bundle catalog.

## Versioning

Subflows should be versioned independently (e.g., `subflow_id@1.2.0`).
Parent flows should reference a specific version to ensure stability.

## Validation Rules

- Subflow entrypoint input/output schemas must match the parent node schemas.
- Cycles through subflows are disallowed (no recursion) in 0.1.
- If parent flow requires `durability=strong`, subflow must also be fully checkpointable.

## Packaging

Subflows can be distributed as:

- Rust crates with `workflow_bundle!` export and entrypoint schemas.
- Bundles included in a host catalog (e.g., compiled into the binary).

In both cases, compile-time linking is required for production builds.
