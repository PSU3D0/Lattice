Status: Draft
Purpose: notes
Owner: Core
Last reviewed: 2025-12-12

# Work Log — Lattice Foundations

> Detailed record of repository setup and implementation progress. Use this to orient new contributors and agents; each entry references the relevant design documents for context.

## Latest State (Authoritative Snapshot) — 2025-12-12

Implemented (high-level):
- Flow IR core types + diagnostics registry (`dag-core`) and macro authoring surface (`dag-macros`).
- Validator (`kernel-plan`) enforcing baseline topology + effects/determinism/idempotency rules.
- Executor (`kernel-exec`) with capture backpressure + streaming support; Axum host bridge (`host-web-axum`).
- CLI for graph checking + local run/serve flows; S1 echo and S2 SSE examples.
- Capability hint registry + multiple capability providers (HTTP, KV/blob via OpenDAL, Redis queue/dedupe/cache), plus queue bridge.
- Metrics catalog + runtime/host/CLI instrumentation; property/fuzz coverage for key invariants.

Roadmap-of-record:
- `impl-docs/roadmap/epics.md`.
- Older “phase plan” references in this log are historical pointers only (the monolithic plan is superseded).

Deferred / mostly-stubbed:
- Registry/certification system, policy engine, importer (n8n), Temporal host, marketplace surfaces.

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

Completed scope aligns with early contract/runtime milestones; references to `impl-docs/impl-plan.md` below are historical (roadmap-of-record is `impl-docs/roadmap/epics.md`):

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

Refer to `impl-docs/roadmap/epics.md` for full detail; the phase framing below is historical context:

1. **Phase 2 — Kernel runtime & Web host (RFC §6, §4.7–§4.9):**
   - Implement `kernel-exec` scheduler, bounded channel/backpressure logic, cancellation propagation, and inline cache stubs.
   - Flesh out `capabilities` crate with typestate traits (HTTP, KV, blob, cache, dedupe, clock).
   - Build `host-web-axum` to serve Web profile workflows (S1/S2), including SSE streaming and request facets.
   - Provide basic instrumentation hooks (tracing, metrics) to satisfy RFC §14.
   - CLI milestone: `flows run local` executing `examples/s1_echo` with latency target (`impl-docs/impl-plan.md` Phase 2 exit criteria).

2. **Phase 3 — Queue profile, dedupe, idempotency harness (RFC §6.4, §5.2):**
   - Implement `bridge-queue-redis`, `cap-dedupe-redis`, `cap-cache-redis`, `cap-blob-fs`.
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

## Week 3 — Phase 2 Runtime Kickoff

- Implemented the first slice of the in-process executor (`crates/kernel-exec`): node registry, bounded edge channels, cancellation propagation, and semaphore-governed capture backpressure aligned with RFC §6.1–§6.5. Captured outputs now release permits only when drained via `FlowInstance::next`.
- Added async unit coverage: `backpressure_applies_when_capture_is_full` verifies capture queue saturation stalls producers, and `cancellation_propagates_on_error` checks that node failures cancel downstream work. Command: `CARGO_TARGET_DIR=.target cargo test -p kernel-exec`.
- Updated Flow IR emission to include `NodeIR.identifier`, enabling runtime handler lookup without re-parsing macro metadata.
- Next steps:
  1. Extend `capabilities` with shared context accessors plus in-memory cache/clock hints (RFC §6.3) so the executor can wire resources.
  2. Stand up `host-web-axum` with HttpTrigger/Respond, SSE streaming, and structured deadline handling (user stories S1/S2).
  3. Expose `flows run local/serve` in the CLI and run S1/S2 end-to-end against the new runtime (Phase 2 exit criteria).
  4. Document runtime metrics emission expectations and wire structured logging once the host is in place.

## Week 3 — Axum Host Integration & CLI Serve

- Introduced `HostHandle` in `crates/host-web-axum`: wraps the Axum router, exposes `into_service()` returning a `tower::make::Shared` make-service, and provides `spawn()` for background servers. Added `RouteConfig` helpers for deadlines/resources and ensured the router resolves to `Router<()>` for `axum::serve` compatibility.
- Wired `flows run serve` to the host through Tokio's multi-thread runtime. The command now binds a `TcpListener`, builds the host from example metadata, serves the `/echo` route, and listens for `Ctrl+C` via `tokio::signal::ctrl_c` before graceful shutdown.
- Added integration coverage in `crates/cli/tests/`: `run_local.rs` shells out with `assert_cmd` to verify payload normalization, while `serve.rs` spins up the Axum host on an ephemeral port and uses `reqwest` + `tokio::time::timeout` to assert round-trip JSON responses.
- Normalised the S1 example (`examples/s1_echo`) to register node handlers under the correct crate path and mark `Responder` as `Pure/Strict`, keeping Plan validator `DAG004` satisfied.
- Updated `impl-plan.md` Phase 2 test matrix to reference the new CLI/HTTP harnesses so future agents maintain the regression coverage.
- Next steps: extend the same pattern to S2 SSE flows once stream surfaces land, emit effect/determinism hints from macros automatically, and introduce capability default registries so runtime validations no longer rely on manual test plumbing.
- Test plan expansion:
  - New `trybuild` fixtures where a node omits effectful metadata; macro-inferred hints should trigger `EFFECT201`/`DET302` without manual hint wiring.
  - Macro unit tests (Rust `#[test]`) ensuring `NodeSpec::effect_hints`/`determinism_hints` include canonical IDs for HTTP read/write, clock, cache, and custom aliases.
  - Validator regression tests that feed the generated `NodeSpec` into `kernel-plan` to confirm diagnostics surface with macro-produced hints.
  - CLI smoke test invoking `flows graph check` on an example with conflicting hints to verify the richer diagnostics path emits the expected code/summary.
  - Future SSE coverage once S2 lands to ensure streaming connectors inherit determinism hints automatically.

Maintaining this log alongside implementation ensures every phase has traceable context, code touchpoints, and clear next actions for agents joining mid-stream.

## Week 4 — SSE Streaming & S2 Example

- Extended the executor with streaming captures: `FlowExecutor::run_once` now returns an `ExecutionResult` enum (`Value` or `Stream`), with `StreamHandle` holding the active `FlowInstance` and releasing backpressure permits only when the stream completes or is dropped. New tests `streaming_capture_emits_events` and `dropping_stream_cancels_run` exercise incremental consumption, cancellation, and error propagation.
- Added `NodeRegistry::register_stream_fn` plus `StreamHandle` helpers, enabling nodes declared with `#[node]` to emit `NodeOutput::Stream`. Streaming hints honour `CancellationToken::cancelled_owned()` so dropping an SSE client tears down the producer.
- Implemented SSE hosting in `crates/host-web-axum`: `streaming_response` wraps `StreamHandle` into `axum::response::Sse`, maps JSON payloads into `Event::data`, and hearts-beat every 15s. Added integration coverage (`serves_sse_stream`) verifying `text/event-stream` responses and event contents.
- Introduced the `examples/s2_site` crate: trigger/snapshot/stream nodes, async instrumentation via `async_stream::stream!`, executor wiring registering the streaming handler, and unit tests validating IR structure. Added `src/bin/dump_ir.rs` to emit prettified Flow IR for schema snapshots.
- CLI upgrades:
  - `flows run local` gained `--stream`, printing JSON lines for streaming captures and gating SSE examples unless the flag is supplied.
  - `flows run serve` now supports S2 via POST + JSON payload, and new integration tests (`serve_streaming_route_emits_sse`) assert SSE delivery end-to-end.
  - `load_example` metadata tracks per-example HTTP method and streaming status to drive both CLI commands.
- Schema/doc updates: captured Flow IR under `schemas/examples/s2_site.json`, documented streaming surfaces in the RFC §6.4/§14 notes, and marked the Phase 2 SSE DoD as complete in `impl-plan.md`.
- Next steps:
  1. Auto-emit effect/determinism hints from macros when streaming connectors declare resources (currently tests seed hints manually).
  2. Expand capability registry coverage (HTTP read/write, cache, clock) so validators catch mismatches without bespoke fixtures.
  3. Instrument runtime metrics per RFC §14 once streaming execution paths are stable.
  4. Add property/fuzz tests for validator hint combinations and executor backpressure once the capability metadata lands.

## Week 5 — Capability Hint Expansion

- Broadened the capability hint taxonomy (`crates/capabilities/src/hints.rs`) to recognise `resource::kv`, `resource::blob`, `resource::queue`, and `resource::db::read` aliases. `dag-macros` now auto-register constraint metadata and surface `EFFECT201`/`DET302` diagnostics for queue publishes and blob access via new `trybuild` fixtures.
- Introduced in-memory capability stubs (`MemoryKv`, `MemoryBlobStore`, `MemoryQueue`) plus error/trait definitions so executors and tests can exercise KV/Blob/Queue behaviours without platform SDKs. `ResourceBag` exposes the new capabilities with builder helpers and regression tests.
- Registered canonical effect/determinism constraints (`capabilities::db::ensure_registered`, etc.) and updated the RFC/implementation plan to document the standard hint IDs consumed by validators and Studio policy tooling.
- Updated `impl-plan.md` Phase 2 scope to note the expanded registry/stub work; refreshed the work log and RFC sections on capability metadata so downstream agents understand the default coverage.
- Next steps:
  1. Teach platform adapters (Workers KV/R2, Neon Postgres) to reuse the shared traits and register platform-specific hints.
  2. Integrate the new capability accessors into executor contexts once runtime metrics land.
  3. Layer property tests over the validator to fuzz combinations of kv/blob/queue hints and ensure diagnostics remain stable.

## Week 6 — Runtime Metrics & CLI Summaries

- Instrumented the executor with `HostMetrics` and friends: per-request guards increment/decrement `lattice.executor.*` gauges/counters, queue depth tracking now rides RAII permits, and new unit tests under `kernel-exec` verify histogram/counter updates via `metrics-util::DebuggingRecorder`.
- Extended the Axum host to emit `lattice.host.*` telemetry (request latency, inflight gauges, SSE client counts, deadline breaches). Streaming responses wrap `StreamHandle` with an `async_stream` guard so gauges drop when clients disconnect, and fresh tests (`records_host_metrics_for_success`) assert the HTTP/SSE paths publish metrics.
- Added `impl-docs/metrics.md` to catalogue every metric (`executor`, `host`, `cli`) with labels/units, and refreshed RFC §14 to point at the catalog.
- CLI upgrades: `flows run local` now clears the recorder per invocation, captures a `RunSummary` (duration, per-node stats, stream counts), prints a human-readable summary to stderr, and exposes `--json` for structured `{ result, summary }` output. Integration coverage in `crates/cli/tests/run_local.rs` checks both pretty prints and JSON payloads. JSON mode is rejected when combined with `--stream` to keep output deterministic.
- Recorded CLI-level metrics (`lattice.cli.*`) after each run (duration histogram, nodes succeeded/failed counters, capture-emitted totals) so future observability backends can ingest high-level run data without scraping stdout.
- Next steps:
  1. Thread metrics export hooks into future deployment targets (e.g., Prometheus exporter behind a flag) once we stabilise control plane requirements.
  2. Fold the same summary/recorder pattern into `flows run serve --inspect` once interactive tooling lands.
  3. Explore structured logging spans for CLI runs so summaries and executor traces share the same `run_id` for downstream correlation.

## Week 7 — Property & Behavioural Fuzzing

- Executor invariants: added a proptest-driven scenario (`flow_instance_respects_capture_capacity`) that prefills capture queues, forces backpressure, and asserts permit accounting never exceeds channel capacity while retries succeed after draining.
- Streaming order guarantees: `host-web-axum` now fuzzes SSE responses (`sse_stream_preserves_event_sequence`) with arbitrary integer payloads, ensuring event ordering and guard teardown remain correct under varied inter-arrival patterns.
- Validator coverage: introduced `fuzz_registry_hint_enforcement`, generating combinations of effect/determinism hints across HTTP/DB/KV/Blob/Queue domains and randomly downgrading declared guarantees to assert diagnostics surface iff the lattice requirements are violated.
- CLI robustness: layered a property test over `flows run local --example s1_echo` that synthesises mixed-case, whitespace-heavy, and Unicode payloads, verifying normalisation behaviour matches the S1 contract for every sampled string.
- Documentation: Phase 2’s property-test deliverable is now marked complete in `impl-plan.md`; future work shifts to long-haul fuzzing (queue profile, plugin registration churn) as the runtime expands.
- Bridge/host split groundwork: introduced the `host-inproc` crate with canonical `Invocation` metadata and refactored `host-web-axum` into a thin HTTP bridge that delegates execution to the shared runtime, setting the stage for environment-specific bridges (Redis, Workers, Lambda) to reuse the same in-process host.
- Axum bridge now forwards HTTP metadata (`http.method`, `http.path`, headers, parsed query params, `auth.user`) into `InvocationMetadata`, and `host-inproc` exposes an `EnvironmentPlugin` trait so platform-specific adapters (Lambda, Workers) can react to the enriched context. Tests cover metadata propagation and plugin hook invocation, and `examples/s1_echo` demonstrates the pattern with a mock Auth0-style middleware that enriches responses with the authenticated user.

## Week 8 — Redis capability consolidation

- Reorganised Redis-specific capability crates beneath `crates/contrib/redis/`, introducing a shared `cap-redis` crate that centralises connection pooling, namespacing, and concurrency limits. Updated workspace membership, READMEs, and surface docs to reflect the new layout.
- Extended the capability registry with an explicit `dedupe` module: registered effect/determinism constraints, expanded `ResourceBag`/`ResourceAccess`, and taught the hint inference table to emit `resource::dedupe::*` metadata so macros auto-propagate requirements for dedupe bindings.
- Implemented `cap-dedupe-redis` atop the shared factory with atomic `SET NX PX` semantics, TTL floor enforcement, namespaced key encoding, and lightweight unit tests covering TTL and key derivation.
- Implemented `cap-cache-redis` using the shared factory, reusing hex-encoded namespacing, honoring default TTLs, and guarding async Redis calls behind runtime-aware blocking helpers to satisfy the synchronous cache trait.
- Updated crate READMEs to reflect the completed Redis command plumbing and refined upcoming work (instrumentation, integration harnesses, queue bridge wiring). Workspace `cargo check` stays green with the new structure.
- Delivered the initial `bridge-queue-redis` skeleton: enqueue serialisation, worker spawning on top of `host-inproc`, Redis list polling with `BRPOPLPUSH`, and round-trip tests for queue envelopes. The bridge now exposes a usable API for higher-level integration work in Phase 3.
- Next steps: extend the bridge with visibility timeouts + dedupe hints, add Redis-backed integration tests, and hook queue execution metrics into the runtime instrumentation before progressing to the idempotency harness.

## Week 9 — Queue leases & KV idempotency harness

- Upgraded `bridge-queue-redis` with visibility-timeout leasing, Redis processing lists, periodic lease extension, and a background reaper that returns expired messages to the ready queue. Queue bridges now emit metrics under the `lattice.queue.*` namespace (enqueue/dequeue, duplicates, requeues, lease events, inflight gauge).
- Added optional dedupe enforcement: queue messages carry optional dedupe metadata extracted from invocation labels/extensions, and bridges consult any registered `DedupeStore` before executing. The bridge records duplicate suppression metrics and releases dedupe keys on failure paths.
- Extended the validator with `EXACT001`/`EXACT002`/`EXACT003` diagnostics: Exactly-once edges now require both a dedupe capability binding and idempotency metadata (key + TTL meeting the 300 s minimum). Flow IR’s `IdempotencySpec` gained an optional `ttl_ms`, schema/docs were updated, and new validation tests cover missing bindings, keys, and under-sized TTLs.
- Implemented spill-to-blob buffering in the executor with a tempdir-backed spill store, `FlowMessage::Spilled` rehydration, and a queue integration test proving overflow messages persist while downstream workers catch up. Validator now emits `SPILL001`/`SPILL002` when flows enable spilling without a bounded buffer or blob capability hint, and the diagnostics/doc registry reflects the new codes.
- Added an initial KV-centric idempotency harness (`testing-harness-idem`) that exercises the `KeyValue` trait (using `MemoryKv`) to verify duplicate blocking and TTL expiry. The harness produces a `HarnessReport` consumed by future certification suites.
- Documentation updates: queue metrics catalogued in `impl-docs/metrics.md`, bridge README refreshed with new behaviour, and the Phase 2 work log now captures the queue bridge progress.

## Week 10 — OpenDAL capability scaffold

- Introduced `cap-opendal-core`, a shared helper crate that mirrors OpenDAL’s service/layer feature flags, exposes an async `OperatorFactory` trait, and provides `SchemeOperatorFactory` plus `OperatorLayerExt` so downstream capability crates can assemble operators without duplicating boilerplate.
- Centralised error translation via `OpendalError`, mapping `opendal::ErrorKind` into portable capability errors that future blob/KV adapters can reuse.
- Documented the new crate, refreshed `impl-docs/opendal-capability-plan.md`, and recorded the scaffold in the Cloudflare host design notes to show how bridge crates will pull from the shared OpenDAL core.
- Landed `cap-blob-opendal` with `OpendalBlobStore`, builder hooks (`with_layer`, `map_operator`), and memory-backend tests; docs updated so Phase 2 of the OpenDAL plan now points at the implementation.
- Added `cap-kv-opendal`, exposing `OpendalKvStore` with capability metadata (`KvCapabilityInfo`), Tokio-friendly blocking adapters, and namespace TTL validation; updated plan/docs to mark Phase 3 complete and broaden host runtime references to cover both blob and KV providers.
- Converted the shared `KeyValue` capability trait to async (`capabilities/src/lib.rs`), updated `MemoryKv`, the idempotency harness, kernel resource tests, and the OpenDAL-backed store to await operations, and introduced `context::with_current_async` so node implementations can use async capability calls without manual plumbing.

## 2025-12-12 — Docs consolidation & encrypted private docs

- Consolidated doc layout: canonical specs in `impl-docs/spec/`, ADRs in `impl-docs/adrs/`, and roadmap/epics in `impl-docs/roadmap/`. Older longform + MCP notes live under `impl-docs/archive/` (including `impl-docs/archive/mcp/`).
- Cloudflare adapter notes remain under `impl-docs/cloudflare/` for implementation context but are not treated as canonical contract docs.
- Private docs workflow: plaintext stays gitignored under `private/impl-docs/`; commit only ciphertext under `impl-docs/_encrypted/` (`mise run encrypt`, `mise run decrypt`, `python scripts/crypto.py status`).
- Added repo-local pre-commit guardrails for `private/` + encrypted-doc checks; install via `mise run hooks-install` (remove via `mise run hooks-uninstall`, verify via `mise run hooks-status`).

## 2025-12-13 — Capability preflight (Epic 01.2)

- Implemented host-level capability preflight in `host-inproc`: required capability domains are derived from Flow IR `effectHints[]` and checked against the configured `ResourceBag` before execution.
- Standardized runtime diagnostic code `CAP101` (missing capability binding) and surfaced it through the Axum host error envelope (`{ error, code, details.hints[] }`).
- Added conformance tests:
  - `host-inproc`: preflight fails for missing KV/HTTP-write/dedupe bindings and succeeds once the provider is present.
  - `host-web-axum`: HTTP error body includes `code = CAP101` and the missing hint list; SSE endpoints emit an `event: error` payload carrying the same `{ code, details.hints[] }`.
- Commands exercised:
  - `cargo test -p host-inproc`
  - `cargo test -p host-web-axum`
  - `cargo test --workspace`

Learnings
- Preflight should derive requirements from `effectHints[]` only (not `determinismHints[]`) to avoid nondeterminism-only domains like clock/RNG.
- Treat unknown `resource::*` effect hints as missing (fail-fast) so new domains cannot silently ship without bindings.
- Host-level preflight can run per-request for correctness; hosts MAY also preflight once at startup to fail before serving traffic.

## 2025-12-14 — Edge timeout macro primitive (Epic 01.3)

- Extended `workflow!` to support `timeout!(from -> to, ms = <int>)` and emit `EdgeIR.timeout_ms`.
- Added `dag-core::FlowBuilder::set_edge_timeout_ms` so macro expansion can mutate existing edges.
- Added validation gate: `kernel-plan` rejects `timeout_ms = 0` with diagnostic `CTRL101`.
- Added tests:
  - `dag-macros`: compile-fail UI coverage for invalid `timeout!` usage + unit test verifying `timeout_ms` emission.
  - `kernel-plan`: `edge_timeout_requires_positive_budget` asserts `CTRL101`.
- Commands exercised:
  - `cargo test -p dag-macros`
  - `cargo test -p kernel-plan`
  - `cargo test --workspace`

## 2025-12-14 — Edge delivery/buffer/spill macro primitives (Epic 01.3)

- Extended `workflow!` to support `delivery!`, `buffer!`, and `spill!`, emitting edge controls (`EdgeIR.delivery`, `EdgeIR.buffer`).
- Added FlowBuilder edge mutators:
  - `set_edge_delivery`
  - `set_edge_buffer_max_items`
  - `set_edge_spill_tier`
  - `set_edge_spill_threshold_bytes`
- Added validation gate: `kernel-plan` rejects `buffer.max_items = 0` with diagnostic `CTRL102`.
- Added tests:
  - `dag-macros`: unit tests for emitted `delivery/buffer/spill` values, validator regression for `EXACT00*`/`SPILL001`, and trybuild fixtures locking invalid syntax, missing edges (`DAG206`), duplicates (`DAG207`), and wrong literal kinds.
  - `kernel-plan`: `edge_buffer_requires_positive_max_items` asserts `CTRL102`.
- Commands exercised:
  - `cargo test -p dag-macros`
  - `cargo test -p kernel-plan`
  - `cargo test --workspace`

## 2025-12-14 — Switch control surface (Epic 01.3)

- Extended `workflow!` to support `switch!`, emitting a `ControlSurfaceIR` with `kind = "switch"` and config shape matching `impl-docs/spec/control-surfaces.md`.
- Added validator enforcement for switch surfaces:
  - `CTRL110`: invalid/malformed switch config shape (missing fields, invalid JSON Pointer, targets list mismatch).
  - `CTRL111`: switch surface references a `source -> target` edge that is missing.
  - `CTRL112`: multiple switch surfaces reference the same `source` alias.
- Implemented runtime routing in `kernel-exec`: after the source node produces a JSON value, evaluate `selector_pointer`, pick case/default, and schedule only the selected branch.
- Surfaced host mapping for switch-related failures in the Axum bridge:
  - Unsupported surfaces -> `CTRL901`.
  - Invalid switch config at runtime -> `CTRL110`.
- Added tests:
  - `dag-macros`: unit test for emitted control surface config/targets + trybuild fixtures for invalid `switch!` usage (missing edges, duplicate sources, duplicate case keys).
  - `kernel-plan`: unit tests for missing-edge (`CTRL111`) and duplicate-source (`CTRL112`).
  - `kernel-exec`: unit test that only the selected branch executes.
- Commands exercised:
  - `cargo test -p dag-macros`
  - `cargo test -p kernel-plan`
  - `cargo test -p kernel-exec`
  - `cargo test -p host-web-axum`
  - `cargo test --workspace`

## 2025-12-14 — If control surface (Epic 01.3)

- Extended `workflow!` to support `if_!`, emitting a `ControlSurfaceIR` with `kind = "if"` and config shape matching `impl-docs/spec/control-surfaces.md`.
- Added validator enforcement for if surfaces:
  - `CTRL120`: invalid/malformed if config shape (missing fields, invalid JSON Pointer, targets list mismatch).
  - `CTRL121`: if surface references a `source -> target` edge that is missing.
  - `CTRL122`: multiple if surfaces reference the same `source` alias.
- Implemented runtime routing in `kernel-exec`: after the source node produces a JSON value, evaluate `selector_pointer` as boolean and schedule `then` or `else`.
- Updated diagnostics registry + docs (`dag-core`, `impl-docs/error-codes.md`, `impl-docs/spec/control-surfaces.md`) with the new `CTRL12*` codes.
- Added tests:
  - `dag-macros`: unit + trybuild fixtures for invalid `if_!` usage (missing edges, duplicate sources, invalid keys/pointer types).
  - `kernel-plan`: unit tests for missing-edge (`CTRL121`), duplicate-source (`CTRL122`), and config shape (`CTRL120`).
  - `kernel-exec`: unit tests for then/else routing and selector_pointer error cases.
- Commands exercised:
  - `cargo fmt --all`
  - `cargo test --workspace`
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`

## 2025-12-15 — Resource catalog + bindings plan spec

- Added a deployment-side resource catalog + bindings lock format to support shared instances, provider selection, and environment-specific realization without embedding secrets.
- Defined provider-kind and wrapper-kind registry boundaries so plugins can validate `provides`, `connect`, and isolation config via JSON schema.
- Documented `bindings.lock.json` as machine-generated output with a deterministic `content_hash` to discourage manual edits.
- Linked the catalog spec from `impl-docs/spec/capabilities-and-binding.md` and captured the intent/realization split (catalog vs lock).


## 2025-12-15 — Reserved surfaces: structural validation + deterministic reject (Epic 01.4)

- Clarified reserved-surface runtime handling in `impl-docs/spec/control-surfaces.md`: reserved runtime-semantic surfaces must fail deterministically when unimplemented (`CTRL901`), while metadata-only surfaces (Partition) may be ignored in 0.1.
- Added validator enforcement for reserved surface shapes in `kernel-plan`:
  - `CTRL130`: malformed config (non-object, missing keys, invalid `v`, invalid pointer/strategy).
  - `CTRL131`: unknown node alias referenced by reserved surface.
  - `CTRL132`: missing edge referenced by reserved surface (e.g. `partition.edge`, `for_each` source->body).
- Updated runtime behavior in `kernel-exec`: `Partition` surfaces are treated as metadata-only and no longer cause instantiation failure; other unsupported surfaces still fail deterministically (`CTRL901`).
- Added tests:
  - `kernel-plan`: reserved-surface structural checks for for-each/partition/rate_limit.
  - `kernel-exec`: partition surface does not block instantiation; unsupported surfaces still do.
- Updated diagnostics registry + docs (`dag-core`, `impl-docs/error-codes.md`, `impl-docs/error-taxonomy.md`) with new `CTRL13*` validation codes.
- Commands exercised:
  - `cargo fmt --all`
  - `cargo test --workspace`
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`

## 2025-12-15 — Triggers & entrypoints wiring rules (Epic 01.5)

- Added `policies.lint.allow_multiple_triggers` (default false) to keep agent-authored flows rigid by default (single canonical input), while allowing explicit opt-in for multi-ingress flows.
- Updated `kernel-plan` validation to reject flows with multiple `NodeKind::Trigger` unless policy opt-in is set (`DAG104`).
- Tightened host entrypoint wiring:
  - Axum host validates trigger/capture aliases at startup and refuses to serve on invalid wiring (errors as `UnknownTrigger`/`UnknownCapture`).
- Extended CLI local/serve execution to bind `resource::*` providers and run through the host runtime so preflight (`CAP101`) and `lf.*` metadata behave consistently.
- Commands exercised:
  - `cargo fmt --all`
  - `cargo test --workspace`
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`

## 2025-12-16 — CLI consumes bindings.lock.json (Epic 01.2)

- Added `--bindings-lock <path>` to `flows run local` and `flows run serve` (mutually exclusive with `--bind`).
- Implemented deterministic lock validation: `content_hash` = sha256 of canonical JSON excluding `content_hash` (sorted object keys, preserved array order).
- Built a `ResourceBag` from lock instances per `flow_id`; validates `provides` coverage and rejects unknown instances/flow IDs.
- Implemented minimal provider allowlist: `kv.memory` -> `capabilities::kv::MemoryKv`, `http.reqwest` -> `cap_http_reqwest::ReqwestHttpClient` (read+write).
- Added strict parsing guards: `version == 1`, `generated_at` required, `connect`/`config` must be objects, isolation wrappers rejected (not supported yet).
- Added `flows bindings lock generate` to emit valid lockfiles (incl. `content_hash`).
- Added CLI integration tests covering lock success + failure modes.
- Added an example lock + regen script under `examples/s4_preflight/`.
- Commands exercised:
  - `cargo fmt --all`
  - `cargo test --workspace`
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
