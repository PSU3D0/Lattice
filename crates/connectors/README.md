# Lattice connectors

Connector families live here, one directory per `<vendor>/<family>`. Each surface
connector declares a small set of ops (the ones real templates invoke —
"ops-not-APIs"), ships a `connector.yaml` manifest, and is proven by the fixed
F1 verification harness (`manifest.rs`, `contract.rs`, `runtime.rs`,
`honesty.rs`, `live_smoke.rs`) plus an `examples/local-flow` end-to-end run.

## Building a new connector family

Read these first, in order:

1. **`../../impl-docs/connector-verification-guide.md`** — the F1 harness: every
   proof you must land and the reference implementation to copy.
2. **`github/issues`** — the canonical request-mapped reference family (src +
   all five test files + example). Copy its layout exactly.
3. **`google/sheets`** — the handwritten-semantic pattern (for ops that compose
   payloads or post-process responses) and the shared-auth-role shape.
4. **`../../../ops/packet-template-connector-family.md`** — the connector-family
   work order (fill every blank; defines the file scope allowlist and the
   acceptance ladder).

## Existing ops

See **`../../../ops/connector-catalog.md`** for the full verified table (one row
per op: effects/hints, endpoint + auth role, static-bearer env var, source path,
which example uses it). CHECK IT before building — the catalog already covers
most shortlist templates. Reach/demand ranking: `../../../ops/n8n-demand-analysis.md`.

Surface families: `airtable`, `discord`, `github/issues`, `google/{gmail,sheets,drive}`,
`hunter`, `llm/core`, `notion`, `slack/core`, `telegram`, `formualizer/sheetport`.
Support crates (no authorable op): `google/platform`, `zoom/platform`.

## Conventions

- **Auth is bearer + env-locked:** `LATTICE_CONNECTOR_AUTH_<ROLE>`; endpoints
  override via `LATTICE_CONNECTOR_ENDPOINT_<PROFILE>_BASE_URL`. Google
  gmail/sheets/drive share ONE role `google_workspace_auth`.
- **Effect hints use `capabilities::*::HINT_*` constants**, never
  `"resource::..."` string literals (CI hint-gate + honesty tests enforce it).
- **Token-in-path/query providers** (telegram, discord, hunter) resolve the
  bearer via a probe request and splice it into the URL — see those crates and
  `../../../ops/clone-playbook.md` §3.

The clone-engine dispatch entry point is `../../../ops/farming-guide.md`.
