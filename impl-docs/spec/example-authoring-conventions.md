Status: Draft
Purpose: authoring-conventions / scaffold-input
Owner: Runtime
Last reviewed: 2026-04-03

# Example Authoring Conventions (0.1.x)

This document captures the current **best-practice shape** for new Lattice
examples.

It exists to reduce repeated one-off example structure decisions and to provide
an explicit target for future scaffolding/templating work.

This is intentionally a **conventions doc first**, not a promise that every
current example already conforms perfectly.

## Scope

This document covers:
- first-party examples under `examples/`
- connector-owned runnable examples under connector crates
- author/agent conventions for crate shape, features, tests, and verification

It does **not** define a stable public `flows new` CLI yet.
That should come later, after another serious example is built through these
conventions.

## Primary goals

A new example should be:
- easy to inspect
- easy to run locally
- truthful about deployment/runtime needs
- portable where the workflow semantics justify portability
- easy for agents to scaffold consistently
- hard to accidentally overfit to one host or one ad hoc harness

## Example categories

## 1. Core first-party examples

Location:
- `examples/<name>/`

Use this for:
- canonical platform examples
- capability-first examples
- AI/product-shape examples
- portability reference examples

Examples:
- `examples/s1_echo`
- `examples/s11_lead_intake`
- `examples/s12_sheetport_quote`

## 2. Connector-owned runnable examples

Location:
- `crates/connectors/<family>/.../examples/local-flow/`

Use this for:
- proving a connector family in a realistic but focused scenario
- validating bindings/runtime behavior for one connector surface
- keeping connector-specific examples near the connector implementation

These examples should still follow the same broad conventions as core examples:
- standard `bundle()` path
- minimal public surface
- native harness logic in tests where possible

## The default stance

### Prefer a real example crate over ad hoc test-only wiring

If a workflow represents a meaningful user/operator/deployment story, prefer a
real example crate rather than only a test fixture.

### Prefer standard `bundle()` over bespoke example entry helpers

Examples should use the macro-generated standard bundle path.
Do **not** introduce one-off helpers like:
- `example_bundle()`
- bespoke public `run_flow()` wrappers
- public mock harness helpers that mix demo plumbing into the example surface

### Prefer explicit topology over hidden magic for flagship examples

For serious examples, especially AI examples, prefer graph-visible major stages.
Do not hide the whole workflow inside one opaque custom node unless that is the
specific point being demonstrated.

## Crate shape

## Minimal crate metadata

Every new example crate should include:
- workspace metadata fields (`authors.workspace`, `edition.workspace`, etc.)
- a concise `description`
- a focused crate name following `example-<name>` style

## `lib` / crate type

### Default/basic examples
For native-only or non-bundled examples, a normal library crate may be enough.

### Portable/bundleable examples
If the example is intended to bundle to wasm or act as a serious portability
reference, prefer:

```toml
[lib]
crate-type = ["cdylib", "rlib"]
```

Use this for examples expected to participate in:
- bundle export
- host-wasmtime proof
- Workers/workerd proof

## Feature conventions

### `host-bundle`
If the example needs host registration/runtime glue for native execution,
prefer:

```toml
[features]
default = ["host-bundle"]
host-bundle = ["kernel-exec", "host-inproc"]
flow-registry = ["dag-core/flow-registry", "dag-macros/flow-registry"]
```

Keep host runtime deps optional where possible so the guest/no-default-features
build remains lean.

### Extra native-only features
Use dedicated features for optional native-only proofing, for example:
- `native-smoke`
- `native-xlsx`

Do **not** force optional native-only machinery into the default portable guest
path if it is not part of the portable example contract.

## Package metadata

For serious bundleable examples, include:

```toml
[package.metadata.latticeflow]
flows = ["..."]
default_flow = "..."
```

This should be treated as part of the standard example shape, not ad hoc
metadata.

## File/folder conventions

A serious example should usually have some subset of:

```text
examples/<name>/
  Cargo.toml
  README.md
  src/lib.rs
  payloads/
  scripts/           # optional provisioning helpers
  assets/            # optional static assets/fixtures
  src/bin/           # optional native smoke / focused helper bin
```

## `README.md`

Add a README when the example is more than trivial.
It should explain:
- what the example proves
- route(s) / invocation shape
- required bindings or env for live/native proofing
- any portability caveats
- the main verification commands

## `payloads/`

Include sample payloads for examples that represent real invocation surfaces.
Prefer checked-in deterministic JSON payloads over inline-only docs examples.

## `scripts/`

Use `scripts/` only for real support tasks, such as deterministic fixture
provisioning.
Prefer `uv` for Python helpers.

## Flow authoring conventions

## Keep major semantics visible

Use graph-visible nodes for major workflow stages that matter for:
- retry semantics
- operator understanding
- deployment reasoning
- capture/approval/artifact boundaries

Follow:
- `impl-docs/spec/node-vs-capability-surface.md`

## Keep plumbing local when appropriate

Do not inflate topology with every tiny capability interaction.
Custom Rust nodes remain valid for composition-local logic, but the node
boundary must remain truthful about effects, determinism, and declared connector
operations.

## For connector usage

When a custom node uses a connector internally, prefer declared connector
operations rather than ambient undeclared access.

Follow:
- `impl-docs/spec/connector-op-reuse-and-node-declaration.md`

## For AI examples

For flagship AI examples, prefer:
- explicit graph-visible AI stages first
- bounded agent-loop internals later

Follow:
- `impl-docs/spec/ai-surface-and-layering.md`
- `impl-docs/spec/agent-loop-runtime.md`

## Public surface rules

## Keep mock/native harness code out of the primary example API

Prefer:
- test-only helpers
- feature-gated native smoke binaries
- local helper functions kept private to tests/bins

Avoid exposing public helpers whose only purpose is local mock/demo wiring.

## Preserve route metadata as part of the example contract

If the example is serveable/bundleable, define route metadata intentionally.
Route aliases, method, trigger alias, capture alias, and deadline are part of
example quality, not incidental details.

## Built-in CLI example registration

If the example is meant to be loadable through `flows run local --example ...`
or `flows run serve --example ...`, update the built-in example registry in:
- `crates/cli/src/main.rs`

Today this is still manual via `load_example(...)`.
Treat that manual registration as part of landing a new built-in example.

## Verification tiers

Not every example needs every proof. Choose the smallest honest tier.

## Tier A — local/basic
Use for small core examples.

Expected checks usually include:
- crate tests
- local run proof or graph/entrypoint check as appropriate

## Tier B — serveable product example
Use when the workflow is meant to be served locally.

Expected checks usually include:
- crate tests
- local run or route roundtrip proof
- serve proof

## Tier C — bundleable/portable example
Use when the example is a portability reference.

Expected checks usually include:
- crate tests
- wasm `--no-default-features` check
- bundle proof
- host-wasmtime proof where applicable

## Tier D — Workers-ready flagship
Use for the most serious portability/product examples.

Expected checks may include:
- crate tests
- wasm guest check
- bundle proof
- host-wasmtime proof
- workerd/miniflare proof
- optional live/native smoke if the example is explicitly designed for it

## Naming and story guidance

Prefer names that describe the workflow story, not internal implementation.
Good:
- `lead_intake`
- `document_intake_pipeline`
- `approval_escalation_flow`
- `github_triage_assistant`

Less helpful:
- names centered on one internal helper or transport detail

The example should tell a product/operator story, not just a crate-structure
story.

## What not to do

- Do not create bespoke public `example_bundle()` helpers.
- Do not mix test-only mock harness logic into the public example API unless it
  is genuinely part of the example contract.
- Do not make portable guest builds depend on optional native-only machinery by
  default.
- Do not hide major workflow stages inside one opaque node if the example is
  supposed to teach topology.
- Do not add a built-in example without also thinking through how it is run,
  served, and verified.
- Do not assume a future scaffold command exists; write examples so a future
  scaffold can learn from them.

## Working conclusion

The current standard is:
- **examples are product-shape teaching artifacts, not throwaway demos**
- **standard `bundle()` path is the default**
- **native harness code belongs in tests/bins where possible**
- **portable guest shape should stay lean and explicit**
- **verification tier should match the example’s claimed role**

This document should be treated as the input contract for future scaffold
commands and agent authoring skills.

## Cross references

- `impl-docs/spec/node-vs-capability-surface.md`
- `impl-docs/spec/connector-op-reuse-and-node-declaration.md`
- `impl-docs/spec/ai-surface-and-layering.md`
- `impl-docs/spec/agent-loop-runtime.md`
- `impl-docs/spec/flow-bundles-wasm.md`
