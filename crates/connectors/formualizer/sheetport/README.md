# connector_formualizer_sheetport

Draft scaffold for a future SheetPort connector family in Lattice.

Status:
- sketch only
- not implemented
- added to support architecture exploration around workbook/manifest connection config

Intended direction:
- connector family: `connector.formualizer.sheetport`
- first operation: `connector.formualizer.sheetport.evaluate`
- deployment-bound connection mode is the canonical/default path
- reusable op surface may also support explicit late-bound typed source refs
- connection-local config resolves workbook + manifest sources in bound mode
- blob-backed workbook sources are the design center
- inline manifest is supported for ergonomics

Companion docs:
- crate-local architecture note: `ARCHITECTURE.md`
- wrapper-local notes:
  - `../../../../../ops/connector-resolution-modes-and-preflight-2026-04-01.md`
  - `../../../../../ops/sheetport-connector-ideation-2026-04-01.md`
  - `../../../../../ops/sheetport-bindings-lock-schema-delta-2026-04-01.md`
  - `../../../../../ops/sheetport-connector-first-operation-contract-2026-04-01.md`

This crate currently provides only a compileable API skeleton:
- connection config structs for bound mode
- model-selection structs for bound vs late-bound mode
- generic SheetPort input/output value shapes
- a draft `ops::SheetPortEvaluate`
- a thin canonical node wrapper

Representative local proofs included now:
- integration tests: `tests/resolution_modes.rs`
  - bound connection config resolution
  - invalid bound-config rejection
  - late-bound mode bypassing connection runtime
  - canonical node vs internal-op parity for the current scaffold
- integration tests: `tests/real_execution.rs`
  - real bound-mode workbook evaluation from blob + inline manifest
  - real late-bound execution without connector runtime
  - workspace artifact export of the evaluated workbook
  - missing blob/workspace capability and missing source-object failures
- representative example: `../../../../examples/s12_sheetport_quote/`
  - canonical bound-node flow
  - internal late-bound custom-node flow
  - resolution-aware preflight proof for bound mode
  - workspace export proof via the canonical wrapper
  - deterministic XLSX asset provisioning via `uv run` + `openpyxl`
- example: `examples/resolution_pattern.rs`
  - shows the API shape for bound/default vs explicit late-bound requests

It does **not** yet:
- support richer source kinds beyond the first set
- export workspace artifacts for complex input-binding shapes such as tables/layout selectors
- polish diagnostics and broader portability proofs
