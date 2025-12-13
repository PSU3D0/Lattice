Status: Draft
Purpose: notes
Owner: Core
Last reviewed: 2025-12-12

# impl-docs index (DRAFT)

This folder is the canonical home for LatticeFlow implementation documentation.

Doc hygiene rules (lightweight, so we can iterate fast):

- **Spec** docs are normative (0.1 contract): tooling/tests/hosts/importers can rely on them.
- **ADRs** capture decisions we don't want to relitigate; ADRs constrain/override specs when they conflict.
- **Roadmap** docs are planning artifacts (epics/phases); they do not define contract semantics.
- **Scenarios** are acceptance targets (what must work; backed by fixtures/tests).
- Older/superseded material moves under `archive/` (never deleted, just marked).

Source-of-truth rule (Flow IR shape):

- **Rust is authoritative** for the emitted Flow IR JSON shape (`crates/dag-core/src/ir.rs` serde output).
- The JSON Schema (`schemas/flow_ir.schema.json`) is **emitted** and must match Rust output; drift is a bug.
- Example JSON under `schemas/examples/` is illustrative and must match Rust+schema; legacy shapes must be explicitly marked/archived.

## Start Here

- Roadmap (epics + phases): `roadmap/epics.md`
- 0.1 contract specs:
  - `spec/flow-ir.md`
  - `spec/invocation-abi.md`
  - `spec/control-surfaces.md`
  - `spec/capabilities-and-binding.md`
- ADRs (forever decisions): `adrs/`
- Scenario specs (acceptance targets): `user-stories.md`

## Current Canonical Docs

- Workspace layout + layering rules (canonical subset): `surface-and-buildout.md`
- RFC umbrella: `rust-workflow-tdd-rfc.md`
- Roadmap: `roadmap/epics.md`

Superseded:
- Monolithic plan: `impl-plan.md` (use `roadmap/` instead)

Notes:
- `schemas/examples/etl_logs.flow_ir.json` is a legacy/alternate IR shape; do not treat it as 0.1 contract until explicitly migrated.

## Stable / Mostly-Append-Only

- Error taxonomy: `error-taxonomy.md`
- Error codes: `error-codes.md`
- Metrics conventions: `metrics.md`

## Active Design Tracks

- Host/runtime refactor notes: `phase3-host-runtime-refactor.md`
- OpenDAL capability rollout: `opendal-capability-plan.md`
- Cloudflare Workers notes: `cloudflare/` (canonical epics live in `roadmap/`)
- Archived MCP notes: `archive/mcp/`

## Consolidation Status

- 0.1 contract specs are landing under `spec/` (draft; expect to mark canonical once aligned + implemented).
- ADRs are landing under `adrs/` (draft; expect to accept as we implement epics).

Next:
- Review/accept the 0.1 contract specs under `spec/` and promote the ones we agree are canonical.
- Review/accept the first ADR batch under `adrs/`.
- Decompose `cloudflare/` docs into epic phase tickets (keep notes, reduce duplicate plans).
- MCP is currently archived (`archive/mcp/`); if revived, rewrite against the 0.1 specs + ADRs.
