Status: Draft
Purpose: notes
Owner: Core
Last reviewed: 2025-12-12

# impl-docs index (DRAFT)

This folder contains the canonical, public-facing implementation contracts for Lattice.

Doc hygiene rules:

- **Spec** docs are normative (0.1 contract): tooling/tests/hosts/importers can rely on them.
- **ADRs** capture decisions we don't want to relitigate; ADRs constrain/override specs when they conflict.
- **Scenarios** are acceptance targets (what must work; backed by fixtures/tests).

Planning/ideation/strategy docs are intentionally **not committed**. If you have local
private planning notes, they live under `private/` (gitignored).

Source-of-truth rule (Flow IR shape):

- **Rust is authoritative** for the emitted Flow IR JSON shape (`crates/dag-core/src/ir.rs` serde output).
- The JSON Schema (`schemas/flow_ir.schema.json`) is **emitted** and must match Rust output; drift is a bug.
- Example JSON under `schemas/examples/` is illustrative and must match Rust+schema; legacy shapes must be explicitly marked/archived.

## Start Here

- 0.1 contract specs:
  - `spec/flow-ir.md`
  - `spec/invocation-abi.md`
  - `spec/control-surfaces.md`
  - `spec/capabilities-and-binding.md`
  - `spec/checkpointing-and-durability.md`
  - `spec/stdlib-and-node-registry.md`
  - `spec/workspace-capability.md`
  - `spec/subflows.md`
  - `spec/connector-and-plugin-model.md`
  - `spec/credential-provider.md`
- ADRs (forever decisions): `adrs/`
- Scenario specs (acceptance targets): `user-stories.md`

## Stable / Mostly-Append-Only

- Error taxonomy: `error-taxonomy.md`
- Error codes: `error-codes.md`

## Private Docs (Local-only)

The following paths are intentionally gitignored and may not exist in public clones:

- `private/impl-docs/roadmap/`
- `private/impl-docs/tickets/`
- `private/docs/plans/`
