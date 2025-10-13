# Work Log — LatticeFlow Foundations

> Detailed record of repository setup and implementation progress. Use this to orient new contributors and agents; each entry references the relevant design documents for context.

## Week 0 — Workspace Scaffolding & Documentation

- Created the Cargo workspace (`Cargo.toml`) with MSRV 1.90, shared dependency table, and member list matching the layout defined in `impl-docs/surface-and-buildout.md` (§A).
- Added crate stubs across `crates/` with README overviews describing intended scope, next steps, and dependencies. Each README mirrors the expectations laid out in the RFC (`impl-docs/rust-workflow-tdd-rfc.md`) and implementation plan (`impl-docs/impl-plan.md` §Phase 0).
- Generated initial documentation set in `impl-docs/`: 
  - `surface-and-buildout.md`: Authoritative workspace layout and layering rules.
  - `impl-plan.md`: Phase roadmap from macro/IR foundations through plugins, importer, and Studio.
  - `rust-workflow-tdd-rfc.md`: Full technical design for macro DSL, Flow IR, kernel runtime, capabilities, plugins, policy, importer, registry, and testing strategy.
  - `user-stories.md`: Scenario-driven requirements (S1–S8) with acceptance criteria and test hooks.
- Added `schemas/flow_ir.schema.json` and `schemas/examples/etl_logs.flow_ir.json` to anchor Flow IR serialization contracts.
- Prepared `AGENTS.md` with contributor guidance.

## Week 1 — Diagnostic Registry & Core Types

- Implemented the error-code registry (`impl-docs/error-codes.md`) covering all diagnostics referenced in the RFC and implementation plan (e.g. `DAG200`, `EFFECT201`, `IDEM020`).
- Built `dag-core`:
  - Defined diagnostics (registry accessor + `Diagnostic` struct), effects/determinism enums, Flow IR structs (`FlowIR`, `NodeIR`, `EdgeIR`, `ControlSurfaceIR`, etc.), and helper builder API.
  - Added serde/schemars derivations to match `schemas/flow_ir.schema.json`.
  - Introduced `FlowBuilder` and unit tests for IR serialization, Flow ID derivation, and registry documentation sync (`crates/dag-core/src/lib.rs`).
  - Added `prelude` exports to streamline downstream usage (macro crate, examples, validators).

## Week 1 — Macro DSL Foundations

- Implemented `dag-macros` with:
  - `#[node]` and `#[trigger]` attribute macros parsing key metadata (`name`, `effects`, `determinism`, `in`, `out`), defaulting based on node type, and emitting `NodeSpec` constants.
  - Diagnostic handling aligned with registry codes (e.g. missing name -> `DAG001`, unknown effects -> `EFFECT201`, duplicate alias -> `DAG205`).
  - `workflow!` macro parsing flow metadata (`name`, `version`, `profile`, `nodes`, `edges`) and building `FlowIR` via `FlowBuilder`, with compile-time cycle and alias checks.
  - Control surface scaffolding for branching/looping metadata (per RFC §4.9) to be extended in later phases.
- Added trybuild UI tests in `crates/dag-macros/tests/ui/` with expected stderr snapshots to lock diagnostic outputs.
- Provided helper macros for downstream use in examples and future connectors (e.g. node spec accessor functions).


## Week 2 — DSL Re-alignment & Diagnostics

- Refactored `workflow!` to mirror the RFC’s declarative syntax (`name/profile[/version]; let …; connect!(…)`) and reject embedded Rust control flow, ensuring authors use `flow::switch` / `flow::for_each` helpers instead of raw `if`/`match` constructs (`crates/dag-macros/src/lib.rs`).
- Updated trybuild fixtures and stderr snapshots so diagnostic baselines reflect the new syntax (`crates/dag-macros/tests/ui/*`).
- Normalised `examples/s1_echo` to the new DSL and kept CI-friendly unit test coverage (`examples/s1_echo/src/lib.rs`).
- Expanded `impl-docs/error-taxonomy.md` with concrete Rust examples (including typed branching/side-effect scenarios) to help agents map validations to real code paths.

## Week 2 — Validation & Exporters

- Implemented `kernel-plan` validation skeleton (`crates/kernel-plan/src/lib.rs`) covering:
  - Duplicate node aliases (`DAG205`).
  - Edge references to unknown nodes (`DAG201`).
  - Cycle detection via DFS (`DAG200`).
  - Schema compatibility checks for connected ports (`DAG201`).
  - Effectful nodes missing idempotency specs (`DAG004`).
  - Unit tests for happy path, schema mismatch, cycle detection, and idempotency enforcement.
- Added `exporters` crate with:
  - `to_json_value` for pretty-printing Flow IR.
  - DOT graph exporter (`to_dot`) for visual inspection and Studio integration.
  - Unit test verifying DOT output for a simple flow.
- Reworked the `workflow!` macro to match the RFC’s declarative `let` + `connect!` syntax, emitting build-time errors when authors attempt to embed raw Rust control flow (`if`/`match`/`for`/`while`). The previous `nodes {}` / `edges []` prototype is now removed to keep DSL semantics aligned with §4.3 of the RFC.

## Week 2 — Generic payload helpers

- Added the `#[flow_enum]` attribute macro (`crates/dag-macros/src/lib.rs`) that stamps enums with the required serde/schemars derives and `#[serde(tag = "type")]`, making the RFC guidance on constrained generics executable (§4.9.4).
- Created integration and `trybuild` tests (`crates/dag-macros/tests/flow_enum.rs`, `crates/dag-macros/tests/ui/flow_enum_not_enum.rs`) to lock serialisation behaviour and compile-time diagnostics.

## Week 2 — Determinism constraint registry

- Introduced shared determinism (`crates/dag-core/src/determinism.rs`) and effects (`crates/dag-core/src/effects_registry.rs`) constraint registries with built-in hints (clock, RNG, HTTP write) plus public APIs for plugins to register additional constraints at runtime.
- Extended `NodeSpec`/`FlowIR` to carry determinism hints, updated the validator to enforce them via new diagnostic `DET302`, and added kernel-plan tests covering built-in and custom hints (`crates/kernel-plan/src/lib.rs`).

## Week 2 — HTTP capability surface & hint propagation

- Defined the canonical HTTP capability traits and request/response types in `crates/capabilities`, including automatic registration of `resource::http`, `resource::http::read`, and `resource::http::write` hint metadata.
- Implemented `cap-http-reqwest` with a production-ready `ReqwestHttpClient`, end-to-end unit tests (using `httpmock`), and automatic hint registration on construction so flows inherit effect/determinism guardrails without manual wiring.
- Updated `dag-macros` to infer both read/write effect hints and domain-level determinism hints from `resources(...)` declarations, plus added `node_hints` coverage to verify the emitted metadata.

## Week 2 — CLI diagnostics upgrades

- Extended `flows graph check` to surface full diagnostic metadata (severity, subsystem, summary, location) and added a machine-readable `--json` mode for agents/automation (`crates/cli/src/main.rs`).
- Captured unit tests that lock in the textual formatter and JSON payload structure, ensuring future diagnostics stay backwards compatible with tooling expectations.

## Week 2 — CLI & Example Flow

- Replaced CLI stub with functional `flows graph check` command (`crates/cli/src/main.rs`):
  - Accepts Flow IR JSON via file or stdin.
  - Runs `kernel-plan::validate` and prints diagnostics with registry codes.
  - Optional DOT output to stdout or file, and pretty-printed JSON output.
  - Future CLI subcommands will layer onto this structure (per `impl-docs/impl-plan.md` Phase 1 & 2).
- Created `examples/s1_echo` crate implementing the S1 webhook scenario from `impl-docs/user-stories.md`:
  - Trigger, normalize, and responder nodes using macros.
  - `workflow!` definition matching the story.
  - Unit test verifying node aliases and edges.

## Week 2 — Repository Documentation

- Composed an extensive root-level `README.md` summarizing project goals, workspace layout, core crates, authoring flow, CLI usage, testing strategy, and roadmap (references `impl-docs` where appropriate).

## Current Status

Completed scope aligns with Phase 0 and most of Phase 1 in `impl-docs/impl-plan.md`:

- ✅ Workspace scaffold, shared dependency management, MSRV pin (`Phase 0`).
- ✅ Diagnostic registry & sync checks (`Phase 0`).
- ✅ Core Flow IR types, builder utilities, and serde support (`Phase 1`).
- ✅ Macro DSL skeleton with control-surface hooks and trybuild coverage (`Phase 1`).
- ✅ Validator skeleton enforcing DAG topology, port compatibility, and basic idempotency rules (`Phase 1`).
- ✅ Exporters (JSON, DOT) and functional `flows graph check` CLI subcommand (`Phase 1`).
- ✅ S1 example flow crate demonstrating macro usage (`Phase 1`).
- ✅ Root README and detailed work log for onboarding.

Outstanding items for Phase 1 (per `impl-docs/impl-plan.md`):

- JSON Schema + snapshot tests for Flow IR (partially covered; expand exporter tests to reference `schemas/examples/etl_logs.flow_ir.json`).
- Additional validator rules: 
  - `DAG202` (unbound variables), `DAG203` (credentials), `EFFECT201` (capability/effects mismatch), `DET301` (determinism/resource conflicts), etc.
  - Control surface linting (`CTRL001`), policy-driven checks via `policy-engine` once implemented.
- CLI integration test hitting the S1 example once execution runtimes are available.

## Next Steps (Phase 2 & Beyond)

Refer to `impl-docs/impl-plan.md` for full detail; immediate priorities:

1. **Phase 2 — Kernel runtime & Web host (RFC §6, §4.7–§4.9):**
   - Implement `kernel-exec` scheduler, bounded channel/backpressure logic, cancellation propagation, and inline cache stubs.
   - Flesh out `capabilities` crate with typestate traits (HTTP, KV, blob, cache, dedupe, clock).
   - Build `host-web-axum` to serve Web profile workflows (S1/S2), including SSE streaming and request facets.
   - Provide basic instrumentation hooks (tracing, metrics) to satisfy RFC §14.
   - CLI milestone: `flows run local` executing `examples/s1_echo` with latency target (`impl-docs/impl-plan.md` Phase 2 exit criteria).

2. **Phase 3 — Queue profile, dedupe, idempotency harness (RFC §6.4, §5.2):**
   - Implement `host-queue-redis`, `cap-dedupe-redis`, `cap-cache-redis`, `cap-blob-fs`.
   - Extend validator for delivery semantics (`Delivery::ExactlyOnce` guardrails), dedupe requirements, cache policies.
   - Build `testing-harness-idem` duplicate injection harness and wire into CLI (`flows certify idem`).

3. **Phase 4 — Registry & connectors (RFC §10–§11):**
   - Implement `registry-client` (local backend), `registry-cert` (determinism/idempotency/contract harnesses).
   - Define `connector-spec` YAML schema and code generation pipeline.
   - Seed initial connectors (Stripe, SendGrid, Slack) with manifests and contract tests.
   - Extend CLI with publish/certify commands.

4. **Phase 5 & 6 — Plugins & Importer (RFC §9, §12):**
   - Develop `plugin-wasi` and `plugin-python` runtimes with capability enforcement.
   - Implement WASM export packaging, importer for n8n JSON, and harvesting scripts (`flows import n8n`).

5. **Phase 7+ — Studio backend, automation:**
   - Build `studio-backend` API, policy engine integration, automated connector farming flows.

Agents picking up Phase 2 work should start by reading RFC §6 (Kernel runtime) and §4.7–§4.9 (control surfaces) alongside `impl-docs/user-stories.md` S1/S2. The validator crate (`crates/kernel-plan/src/lib.rs`) already exposes `ValidatedIR`, which will feed directly into `kernel-exec` once implemented.

## Upcoming Work — Week 2 Follow-up

- **Macro hint propagation:** implement automatic `effect_hints` / `determinism_hints` emission in `dag-macros` so registry-driven checks fire without manual wiring (`crates/dag-macros/src/lib.rs`, RFC §8.3).
- **Validator-facing trybuild coverage:** extend UI fixtures to capture `EFFECT201` / `DET302` errors when hints disagree with declared metadata, keeping Phase‑1’s TDD mandate intact (`impl-docs/impl-plan.md`, Phase 1 tests).
- **Capability hint seeding:** register default effect/determinism constraints inside foundational capability crates so macros inherit rich metadata out of the box (`impl-docs/surface-and-buildout.md` §§A–B).
- **Schema & doc refresh:** update Flow IR examples and RFC control-surface sections to reflect new hint arrays and ensure artifacts remain in sync (`schemas/flow_ir.schema.json`, RFC §5).
- **CLI diagnostics:** surface the stricter validator output through `flows graph check` to provide actionable author feedback in line with Phase‑1 exit criteria (`impl-docs/impl-plan.md`, Phase 1 exit).

## Helpful References

- **Design & Requirements:**
  - `impl-docs/rust-workflow-tdd-rfc.md` — macro semantics (§4), Flow IR spec (§5), kernel design (§6), capability system (§6.3), plugins (§9), importer (§12), testing (§16).
  - `impl-docs/user-stories.md` — provides end-to-end acceptance tests for S1 (Webhook echo), S2 (SSE site), S3 (Batch ETL), etc.
  - `impl-docs/surface-and-buildout.md` — layering rules and phase DoDs.
  - `impl-docs/impl-plan.md` — phase milestones, CLI targets, harness requirements.
- **Code Pointers:**
  - `crates/dag-core/src/lib.rs` — Flow IR types and tests.
  - `crates/dag-macros/src/lib.rs` — DSL implementation and diagnostics.
  - `crates/kernel-plan/src/lib.rs` — validator logic and unit tests.
  - `crates/exporters/src/lib.rs` — JSON/DOT exporters.
  - `crates/cli/src/main.rs` — CLI command structure for future expansion.
  - `examples/s1_echo/src/lib.rs` — canonical macro usage example.

Maintaining this log alongside implementation ensures every phase has traceable context, code touchpoints, and clear next actions for agents joining mid-stream.
