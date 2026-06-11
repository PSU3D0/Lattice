# Lattice

![Lattice](https://raw.githubusercontent.com/PSU3D0/lattice/main/assets/banner.jpeg)

## Overview

Lattice is a Rust-first workflow automation platform designed to give human and AI authors a typed, policy-aware alternative to low-code orchestrators such as n8n or Temporal JSON definitions. Authors express flows through a macro DSL that expands to:

- Native Rust execution units (`#[def_node]`, `flow!`)
- A canonical Flow Intermediate Representation (Flow IR) captured as JSON
- Validation diagnostics with stable error codes (e.g. `DAG201`, `IDEM020`)
- Execution plans that target multiple hosts (Tokio/Web, Cloudflare Workers, Redis queue, WASM; durable long-running and scripting hosts are planned)

The workspace provides everything needed to go from macro-authored code to running workflows on local executors, queue workers, or edge WASM packs, with gating harnesses for policy, determinism, and idempotency.

## Thesis

Agents make writing code cheap, so keystroke savings are not the product. The product is **compile-verifiability of effects and capability access**: a flow's declared effects and the resources it may touch should be provable from the code, not trusted by convention. Declarative ceremony that encodes a verifiable claim (effects, determinism, idempotency, capability requirements) is intentional and worth its cost; duplicative plumbing (manual registration, copy-pasted client setup) is a bug, because it is where agents introduce silent drift.

Execution economics are Cloudflare-Workers-first: multiple flows ship in one bundle behind a dispatch entrypoint, and on the Workers free tier roughly 1M simple flow executions per month is feasible. The direction this points to is **infra-from-code**: static per-flow requirement manifests, derivable from the bundle without running anything, that let tooling provision the minimum infrastructure a flow actually needs.

## High-Level Goals

- **Code-first:** Rust is the source of truth. Macros add structure without hiding control flow so developers and agents retain full language power.
- **Typed contracts:** All ports, params, capabilities, and policies flow through Flow IR and JSON Schemas, enabling automated tooling and Studio visualisation.
- **Determinism & effects:** Every node declares its effects (`Pure`, `ReadOnly`, `Effectful`) and determinism (`Strict`, `Stable`, `BestEffort`, `Nondeterministic`) so the runtime can enforce caching, retries, and compensations.
- **Policy & compliance:** Capabilities, secrets, and egress are gated at compile time, certification, and runtime. Error codes map directly to policy remediation steps.
- **Extensibility:** A WASM plugin host seam (`host-wasmtime`) and connector specs let external teams or agents extend the platform while preserving sandboxing and evidence trails; embedded scripting plugins are a planned direction.

## Current Maturity Snapshot

### Available today
- Macro-authored flows with compile-time metadata and stable validation diagnostics.
- `flows graph check`, `flows entrypoints check`, `flows run local`, and `flows run serve`.
- In-process execution via `host-inproc` and HTTP serving via `host-web-axum`.
- Run-scoped workspace capability with native and Workers backends.
- Host-owned connector runtime/bindings substrate with local OAuth2 refresh and service-account JWT providers.
- Runnable connector-owned examples for GitHub Issues and Google Sheets.
- Workerd/Miniflare-backed `host-workers` coverage for durability and workspace semantics.

### Still preview / under active buildout
- Workers parity for richer connector binding/provider execution.
- Connector trigger runtime, inbound verifier providers, and activation lifecycle.
- Larger connector farming surface beyond the first serious Google Sheets slice.
- Broader real-world workflow example coverage across multiple domains.

### Planned / not yet started
These are directions, not crates in the workspace today:
- Durable long-running host backend (e.g. a Temporal-style execution target).
- Registry certification (sigstore signing, SBOM snapshots, harness evidence) atop `registry-client`.
- n8n **spec extraction** (mining n8n definitions into agent briefs), not transpilation.
- A WASM plugin host beyond the in-tree `host-wasmtime` seam, and an embedded scripting plugin surface.
- A Studio backend for visualisation/management.

## Workspace Topology

```
/Cargo.toml                       # Workspace manifest (MSRV 1.90, shared deps)
/impl-docs/                       # Contract specs, ADRs, diagnostics, and acceptance stories
/schemas/                         # Flow IR JSON Schema + reference artifacts
/crates/                          # Primary library, runtime, tooling, and adapters
  dag-core/                       # Flow IR types, diagnostics, builder utilities
  dag-macros/                     # Authoring DSL (`#[def_node]`, `flow!`, control surfaces)
  kernel-plan/                    # Flow IR validators + lowering and policy checks
  kernel-exec/                    # Runtime for execution plans, resume, and resource overlays
  capabilities/, cap-*            # Capability typestates + concrete providers
  host-inproc/, host-web-axum/, host-workers/, bridge-queue-redis/  # Runtime + execution bridges
  exporters/                      # Flow IR exporters (JSON, DOT, future OpenAPI/WIT)
  registry-client/                # Registry publication tooling
  connector-spec/, connectors-std/# Connector generation + curated packs
  testing-harness-idem/           # Idempotency harness utilities
  cli/                            # `flows` CLI commands
/examples/s1_echo/                # Canonical S1 webhook example using macros
```

### Authoritative References
- **0.1 contract specs:** `impl-docs/spec/`
- **Flow IR schema:** `schemas/flow_ir.schema.json`
- **User stories & acceptance criteria:** `impl-docs/user-stories.md`
- **Diagnostic registry:** `impl-docs/error-codes.md`

## Key Crates

| Crate | Purpose |
|-------|---------|
| `dag-core` | Canonical types: Flow IR structs, builder helpers, diagnostics, effects/determinism. |
| `dag-macros` | Procedural macros expanding Rust nodes/triggers/flows and emitting Flow IR. Includes trybuild suites for diagnostics. |
| `kernel-plan` | Validation engine enforcing DAG rules, port compatibility, cycle detection, and idempotency preconditions. Produces `ValidatedIR`. |
| `kernel-exec` | In-process executor with scheduling, deadlines, resource overlays, and resume integration points. |
| `exporters` | Flow IR exporters (`to_json_value`, `to_dot`) consumed by CLI and Studio. |
| `flows-cli` | CLI entrypoint for graph validation, entrypoint checks, local execution, and local serving. |
| `capabilities`, `cap-*` | Typestate traits and concrete implementations for HTTP, KV, blob, cache, dedupe, clock, workspace, etc. |
| `host-*` | Host adapters for Axum/web, Cloudflare Workers, and Redis queue workers. |
| `registry-client` | Registry publishing and client integration. |
| `connector-spec`, `connectors-std` | YAML schema/codegen for connectors, curated packs. |
| `testing-harness-idem` | Duplicate injection + evidence capture for idempotency certs. |

## Flow Authoring Path

1. **Define nodes & triggers** using macros in your crate:
   ```rust
   #[def_node(name = "Normalize", effects = "Pure", determinism = "Strict")]
   async fn normalize(event: Order) -> NodeResult<SanitisedOrder> { ... }

   #[def_node(trigger, name = "Webhook")]
   async fn webhook(req: HttpRequest) -> NodeResult<WebhookEvent> { ... }
   ```
2. **Assemble flows** with `flow!`, using `node!(...)` helpers for bindings. The macro emits both Rust wiring and Flow IR JSON artefacts.
3. **Inspect Flow IR** using the CLI:
   ```bash
   cargo run -p flows-cli -- graph check --input flow_ir.json --emit-dot
   ```
4. **Validate** with `kernel-plan::validate` (automatically invoked by CLI) to catch cycles, port mismatches, or missing idempotency keys.
5. **Export** DOT or JSON for Studio/agents via `exporters` crate or CLI options.

The `examples/s1_echo` crate demonstrates a minimal Web profile flow with trigger, inline logic, and responder nodes.

## Flow IR & Diagnostics

- **Serialization:** `dag-core::FlowIR` derives serde/schemars; schema at `schemas/flow_ir.schema.json`.
- **IDs:** `FlowId` derived from `{name, semver}` using UUID v5 (string-encoded for schema friendliness).
- **Nodes:** Capture alias, kind (`Trigger`, `Inline`, `Activity`, `Subflow`), port schemas, effects, determinism, idempotency spec, docs.
- **Edges:** Include delivery semantics (`AtLeastOnce`, `AtMostOnce`, `ExactlyOnce`), ordering, partition key, timeout, buffer policy.
- **Control surfaces:** Document branching (`Switch`, `If`), loops (`ForEach`, `Loop`), temporal hints, rate limits, error handlers.
- **Diagnostics:** All lints/errors map to `impl-docs/error-codes.md`. `dag-core` exports `Diagnostic` and registry accessor for consumers.

Validation highlights implemented in `kernel-plan::validate`:
- Duplicate aliases (`DAG205`)
- Edge references to unknown nodes (`DAG201`)
- Cycle detection via DFS (`DAG200`)
- Port schema compatibility (named schema equality)
- Malformed idempotency declarations (`DAG004`)

Additional rules (delivery requirements, capability overlap, policy waivers) are outlined in the RFC and queued for future phases.

## Runtime, Bridges & Hosts

- **kernel-exec:** In-process executor for validated flows, including resource overlays, run identity plumbing, and checkpoint/resume integration points.
- **host-inproc:** Shared runtime harness used by examples, tests, and higher-level hosts to execute validated flows in-process.
- **host-web-axum:** Axum adapter that mounts HTTP triggers, handles request facets, streaming responses, deadlines, and cancellation propagation.
- **host-workers:** Cloudflare Workers adapter with workerd/Miniflare coverage, DO-backed durability substrate, and Workers workspace wiring.
- **bridge-queue-redis:** Queue bridge for Redis-backed execution lanes; still earlier than the web/native path.
- **host-wasmtime:** Native Wasmtime host seam for running WASM-packed flows off the edge; earlier than the inproc/web/workers paths.
- A durable long-running host backend (Temporal-style) and an embedded scripting plugin surface are planned directions, not present crates.

## Capabilities & Connectors

- **Capabilities:** Traits in `capabilities` ensure compile-time enforcement of effect/determinism contracts (e.g., stable reads require pinned resources). Concrete providers live in `cap-*` crates.
- **Current recommended local stack:** `cap-http-reqwest`, `cap-workspace-fs`, and `cap-opendal` (its `blob` module for filesystem-backed objects and `kv` module for SQLite-backed key-value, where appropriate).
- **Current recommended Workers stack:** `cap-http-workers`, `cap-workspace-workers`, `cap-kv-workers`, and `cap-do-workers`.
- **Important distinction:** workspace is run-scoped scratch; blob is durable artifact/object storage. Keep those capabilities separate even if they share a filesystem-backed implementation locally.
- **Future SQL note:** if Lattice grows a real SQL/DB capability, it should remain separate from `resource::kv` even when a KV provider happens to use SQLite internally.
- **ConnectorSpec:** Declarative YAML schema + Rust codegen for connectors, including manifest metadata (effects, determinism, egress, rate limits, tests).
- **connectors-std:** Umbrella crate re-exporting provider-specific connectors once generated.
- **Registry:** `registry-client` handles publishing/client integration. Certification (sigstore signing, SBOM snapshots, determinism/idempotency/test harness evidence) is a planned layer on top, not a present crate.

## CLI Usage

```bash
# Validate a Flow IR document (from file)
cargo run -p flows-cli -- graph check --input schemas/examples/s2_site.json

# Validate trigger/capture wiring
cargo run -p flows-cli -- entrypoints check --input schemas/examples/s2_site.json

# Execute a built-in example locally
cargo run -p flows-cli -- run local --example s1_echo --payload '{"value":" Hello "}'

# Serve a built-in example over HTTP
cargo run -p flows-cli -- run serve --example s1_echo --addr 127.0.0.1:8080

# Serve a bundled workflow artifact over HTTP
cargo run -p flows-cli -- run serve \
  --bundle /tmp/flow.bundle \
  --addr 127.0.0.1:8080

# Execute the connector-owned Google Sheets example via bindings.lock
cargo run -p flows-cli -- run local \
  --example connector_google_sheets_local_flow \
  --bindings-lock /tmp/google-sheets-live.bindings.lock.json
```

Still planned / incomplete:
- queue-first operator flows and richer bridge commands
- registry certification / publish flows
- importer-driven `n8n` translation path

## Development Guide

### Prerequisites
- **Rust 1.90** (workspace MSRV; `mise.toml` currently pins 1.93.0 for local tooling)
- `mise` for the repo-managed task/toolchain entrypoints
- Optional: Redis (queue profile), Wasmtime (WASM host), Cloudflare `wrangler`/`workerd` (Workers host + tests)

### Common Commands

```bash
# Fast local validation
mise run validate

# Native + wasm validation + secret scan (CI-shaped aggregate)
mise run validate-ci

# Build core crates directly
cargo check -p dag-core
cargo check -p dag-macros
cargo check -p kernel-plan
cargo check -p flows-cli

# Run macro UI tests (requires a writeable target dir; use env var to sidestep sandbox limits)
CARGO_TARGET_DIR=.target cargo test -p dag-macros --test trybuild

# Execute canonical examples
cargo test -p example-s1-echo
cargo run -p flows-cli -- run local --example s1_echo --payload '{"value":" Hello "}'
```

> **Note:** When running tests inside sandboxed environments (e.g., the Codex CLI harness), set a workspace-local `CARGO_TARGET_DIR` to avoid cross-device link errors: `CARGO_TARGET_DIR=.target cargo test ...`.
>
> If you work with encrypted private docs locally, keep `fnox.toml` untracked and start from `fnox.example.toml` rather than committing a real secret-manager config.

### Build tooling

Two optional one-time setup steps make local builds faster and keep the Workers
harness from reinstalling tooling on every invocation:

- **mold linker (Linux, optional but recommended).** `.cargo/config.toml` passes
  `-fuse-ld=mold` to the default linker driver for `x86_64-unknown-linux-gnu`,
  which cuts link time on incremental rebuilds. Install it once:
  ```bash
  sudo apt-get install -y mold   # Debian/Ubuntu; mold 2.x
  ```
  If mold is not installed, native builds fail at link time with
  `cannot find -fuse-ld=mold`; install mold or comment out the `rustflags`
  line in `.cargo/config.toml`. CI installs mold automatically.

- **`worker-build` for the Workers/workerd harness.** The `wrangler.toml`
  `[build]` commands delegate to `scripts/ensure-worker-build.sh`, which uses an
  already-installed `worker-build` instead of running `cargo install` on every
  `wrangler dev`/`deploy`/test. Install it once:
  ```bash
  cargo install worker-build@^0.7
  ```
  If it is missing, the build fails fast with that exact instruction. In CI (or
  by exporting `LATTICE_AUTO_INSTALL_WORKER_BUILD=1`) the script auto-installs it.

### Phased Buildout

Implementation is organised into discrete phases (`impl-docs/impl-plan.md`):
1. **Phase 0:** Workspace scaffold, linting, diagnostic registry (completed here).
2. **Phase 1:** Core types/macros/IR/validator/exporters/CLI (partially implemented).
3. **Phase 2:** In-process executor + Web host + inline caching.
4. **Phase 3:** Queue profile, Redis dedupe/cache, idempotency harness.
5. **Phase 4:** Registry, certification harness, connector spec infrastructure.
6. **Phase 5:** Plugin hosts (WASM, Python) and capability extensions.
7. **Phase 6:** WASM edge deployment + n8n importer.
8. **Phase 7+:** Automated connector farming, Studio backend polish.

Each phase has explicit exit criteria, test suites, and CLI milestones to ensure incremental value delivery.

## Error Handling & Diagnostics

- Diagnostics carry structured metadata: code, subsystem, severity, message.
- Consumers retrieve the registry via `dag_core::diagnostic_codes()`.
- Validation and macro errors should use the canonical codes to enable consistent remediation playbooks and agent automation.
- Runtime diagnostics (e.g., `RUN030`, `TIME015`) will be emitted through the kernel runtime and surfaced in CLI/Studio once implemented.

## Testing Strategy

Outlined in the RFC (`impl-docs/rust-workflow-tdd-rfc.md`, §16):
- **Unit:** Macro expansion, capability typestates, Flow IR serialization, validator rules.
- **Property:** Cycle detection, partition semantics, idempotency keys.
- **Golden:** Example flows (S1 echo, S2 SSE site) compiled and exported.
- **Integration:** HTTP runtime, queue dedupe & spill, Workers (workerd/Miniflare) durability; durable long-running and scripting hosts are planned.
- **Certification harnesses:** Determinism replay, idempotency duplicates, policy evidence.

Current repository includes unit/trybuild coverage for macros, Flow IR builder tests in `dag-core`, kernel-plan validation tests, exporter smoke tests, workerd/Miniflare coverage for `host-workers`, and runnable connector-owned example packages.

## Roadmap & Next Steps

- Expand the real-world example suite across multiple domains, not just substrate demos.
- Harden capability ergonomics for KV/blob/workspace-first authoring patterns.
- Improve Workers parity for connector bindings, auth providers, and connector-bound flows.
- Grow the connector ecosystem beyond GitHub Issues and Google Sheets, especially into richer trigger/webhook families.
- Land clearer stability tiers for scaffold vs preview vs supported public surfaces.
- Open up the planned directions — n8n spec extraction, registry certification, scripting plugin host, and a Studio backend — from the current foundations.

Refer to `impl-docs/impl-plan.md` and `impl-docs/surface-and-buildout.md` for detailed sequencing.

## Contributing

1. Ensure your Rust toolchain is at least 1.90.
2. Run formatting (`cargo fmt`) and linting (`cargo clippy -- -D warnings`).
3. Execute relevant `cargo check` / `cargo test` commands (set `CARGO_TARGET_DIR=.target` if needed).
4. Adhere to the diagnostic registry when adding new validations or lints.
5. Update documentation (README, `impl-docs`, schemas) alongside code changes.
6. Review `AGENTS.md` for contributor guidelines tailored to agent-assisted workflows.

For large features, coordinate across phases to keep milestones achievable and to avoid bypassing gating harnesses (policy, certification, importer SLOs).

## Additional Resources

- `impl-docs/rust-workflow-tdd-rfc.md`: Full technical design (macros, IR, runtime, policy, importer).
- `impl-docs/user-stories.md`: Scenario-driven requirements (S1–S8 and beyond).
- `AGENTS.md`: Contributor guidelines for human/agent collaboration.
- `schemas/examples/etl_logs.flow_ir.json`: Reference Flow IR example matching schema, validators, and exporters.
- `crates/dag-macros/tests/`: Trybuild suites for DSL diagnostics.
- `crates/kernel-plan/src/lib.rs`: Validation implementation & tests (great starting point for additional rules).

---

Lattice is early in its implementation, but the scaffolding above lays the foundation for a robust, typed, policy-aware workflow engine that balances code-first ergonomics with the automation opportunities AI agents demand. Dive into the examples, run the CLI, and extend the crates to bring the next phases to life. Happy building!
