# Lattice

![Lattice](https://raw.githubusercontent.com/PSU3D0/lattice/main/assets/banner.jpeg)

## Overview

Lattice is a Rust-first workflow automation platform designed to give human and AI authors a typed, policy-aware alternative to low-code orchestrators such as n8n or Temporal JSON definitions. Authors express flows through a macro DSL that expands to:

- Native Rust execution units (`#[node]`, `#[trigger]`, `workflow!`)
- A canonical Flow Intermediate Representation (Flow IR) captured as JSON
- Validation diagnostics with stable error codes (e.g. `DAG201`, `IDEM020`)
- Execution plans that target multiple hosts (Tokio/Web, Redis queue, Temporal, WASM, Python gRPC)

The workspace provides everything needed to go from macro-authored code to running workflows on local executors, queue workers, or edge WASM packs, with gating harnesses for policy, determinism, and idempotency.

## High-Level Goals

- **Code-first:** Rust is the source of truth. Macros add structure without hiding control flow so developers and agents retain full language power.
- **Typed contracts:** All ports, params, capabilities, and policies flow through Flow IR and JSON Schemas, enabling automated tooling and Studio visualisation.
- **Determinism & effects:** Every node declares its effects (`Pure`, `ReadOnly`, `Effectful`) and determinism (`Strict`, `Stable`, `BestEffort`, `Nondeterministic`) so the runtime can enforce caching, retries, and compensations.
- **Policy & compliance:** Capabilities, secrets, and egress are gated at compile time, certification, and runtime. Error codes map directly to policy remediation steps.
- **Extensibility:** Plugins (WASM, Python) and connector specs allow external teams or agents to extend the platform while preserving sandboxing and evidence trails.

## Workspace Topology

```
/Cargo.toml                       # Workspace manifest (MSRV 1.90, shared deps)
/impl-docs/                       # RFC, implementation plan, surface layout, user stories
/schemas/                         # Flow IR JSON Schema + reference artifacts
/crates/                          # Primary library, runtime, tooling, and adapters
  dag-core/                       # Flow IR types, diagnostics, builder utilities
  dag-macros/                     # Authoring DSL (`#[node]`, `workflow!`, control surfaces)
  kernel-plan/                    # Flow IR validators + lowering scaffolding
  kernel-exec/                    # (Scaffold) runtime for execution plans
  capabilities/, cap-*            # Capability typestates + concrete providers
  host-inproc/, host-web-axum/, bridge-queue-redis/, host-temporal/  # Runtime + execution bridges
  plugin-wasi/, plugin-python/    # Sandbox runtimes for WASM and gRPC plugins
  exporters/                      # Flow IR exporters (JSON, DOT, future OpenAPI/WIT)
  registry-client/, registry-cert/# Certification + registry publication tooling
  connector-spec/, connectors-std/# Connector generation + curated packs
  policy-engine/                  # CEL-based policy evaluation (scaffold)
  testing-harness-idem/           # Idempotency harness utilities
  cli/                            # `flows` CLI commands
/examples/s1_echo/                # Canonical S1 webhook example using macros
```

### Authoritative References
- **Macro & DSL spec:** `impl-docs/rust-workflow-tdd-rfc.md` (§4)
- **Flow IR schema:** `schemas/flow_ir.schema.json`
- **Implementation plan:** `impl-docs/impl-plan.md`
- **Workspace layout rationale:** `impl-docs/surface-and-buildout.md`
- **User stories & acceptance criteria:** `impl-docs/user-stories.md`
- **Diagnostic registry:** `impl-docs/error-codes.md`

## Key Crates

| Crate | Purpose |
|-------|---------|
| `dag-core` | Canonical types: Flow IR structs, builder helpers, diagnostics, effects/determinism. |
| `dag-macros` | Procedural macros expanding Rust nodes/triggers/workflows and emitting Flow IR. Includes trybuild suites for diagnostics. |
| `kernel-plan` | Validation engine enforcing DAG rules, port compatibility, cycle detection, and idempotency preconditions. Produces `ValidatedIR`. |
| `kernel-exec` | (Scaffold) in-process executor with scheduling, backpressure, and cancellation hooks. |
| `exporters` | Flow IR exporters (`to_json_value`, `to_dot`) consumed by CLI and Studio. |
| `flows-cli` | CLI entrypoint (`flows graph check`, future run/certify/publish subcommands). |
| `capabilities`, `cap-*` | Typestate traits and concrete implementations for HTTP, KV, blob, cache, dedupe, clock, etc. |
| `host-*` | Host adapters for Axios-web, Redis queue workers, Temporal lowering. |
| `plugin-*` | Sandboxed plugin runtimes (WASM/Wasmtime + WIT, Python gRPC). |
| `registry-*` | Publishing, signing, and certification harness integration. |
| `connector-spec`, `connectors-std` | YAML schema/codegen for connectors, curated packs. |
| `testing-harness-idem` | Duplicate injection + evidence capture for idempotency certs. |

## Flow Authoring Path

1. **Define nodes & triggers** using macros in your crate:
   ```rust
   #[node(name = "Normalize", effects = "Pure", determinism = "Strict")]
   async fn normalize(event: Order) -> NodeResult<SanitisedOrder> { ... }
   ```
2. **Assemble workflows** with `workflow!` or attribute sugar for control surfaces. The macro emits both Rust wiring and Flow IR JSON artefacts.
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
- Effectful nodes lacking idempotency metadata (`DAG004`)

Additional rules (delivery requirements, capability overlap, policy waivers) are outlined in the RFC and queued for future phases.

## Runtime, Bridges & Hosts (Scaffold)

- **kernel-exec:** Will host the async scheduler, cancellation tokens, partition routing, backpressure, and spill-to-blob logic. Targets the Web, Queue, Temporal, WASM profiles enumerated in `impl-docs/impl-plan.md`.
- **host-inproc:** Shared runtime harness used by all bridges to execute validated flows within the current process.
- **host-web-axum:** Axum adapter that mounts HTTP triggers, handles SSE streaming, request facet injection, deadlines, and cancellation propagation.
- **bridge-queue-redis:** Redis-based queue bridge managing visibility timeouts, dedupe integration (`cap-dedupe-redis`), rate limits, and fairness scheduling before delegating to `host-inproc`.
- **host-temporal:** Code generation + Rust activity worker bridging Flow IR to Temporal workflows and activity semantics (signals, timers, Continue-As-New).
- **plugin-wasi / plugin-python:** Sandboxed plugin hosts enforcing declared capabilities, memory/timeout budgets, and determinism evidence.

All host crates currently provide scaffolding with README guidance and will be filled out as phases progress.

## Capabilities & Connectors

- **Capabilities:** Traits in `capabilities` ensure compile-time enforcement of effect/determinism contracts (e.g., stable reads require pinned resources). Concrete providers live in `cap-*` crates.
- **ConnectorSpec:** Declarative YAML schema + Rust codegen for connectors, including manifest metadata (effects, determinism, egress, rate limits, tests).
- **connectors-std:** Umbrella crate re-exporting provider-specific connectors once generated.
- **Registry:** `registry-client` and `registry-cert` coordinate publishing, sigstore signing, SBOM snapshots, determinism/idempotency/test harness runs.

## CLI Usage

```bash
# Validate a Flow IR document (from file)
cargo run -p flows-cli -- graph check --input examples/s1_echo.flow_ir.json

# Validate from stdin and emit DOT
cargo run -p flows-cli -- graph check --emit-dot < flow.json

# Validate and write DOT to disk
cargo run -p flows-cli -- graph check --input flow.json --dot flow.dot
```

Future commands (per implementation plan):
- `flows run local` / `flows run serve`
- `flows queue up`
- `flows export wasm`
- `flows certify` (idempotency/determinism/policy harnesses)
- `flows publish` (registry hand-off)
- `flows import n8n`

## Development Guide

### Prerequisites
- **Rust 1.90** (workspace MSRV)
- `cargo fmt`, `cargo clippy`, `cargo test`
- Optional: Redis (queue profile), Wasmtime (plugin host), Temporal dev server (later phase)

### Common Commands

```bash
# Format & lint
cargo fmt
cargo clippy --all-targets --workspace -- -D warnings

# Build core crates
cargo check -p dag-core
cargo check -p dag-macros
cargo check -p kernel-plan
cargo check -p flows-cli

# Run macro UI tests (requires a writeable target dir; use env var to sidestep sandbox limits)
CARGO_TARGET_DIR=.target cargo test -p dag-macros

# Execute example unit tests
cargo test -p example-s1-echo
```

> **Note:** When running tests inside sandboxed environments (e.g., the Codex CLI harness), set a workspace-local `CARGO_TARGET_DIR` to avoid cross-device link errors: `CARGO_TARGET_DIR=.target cargo test ...`.

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
- **Integration:** HTTP runtime, queue dedupe & spill, Temporal workflows, plugin hosts.
- **Certification harnesses:** Determinism replay, idempotency duplicates, policy evidence.

Current repository includes unit/trybuild coverage for macros, Flow IR builder tests in `dag-core`, kernel-plan validation tests, and exporter smoke tests.

## Roadmap & Next Steps

- Extend validator coverage (delivery semantics, capability/policy checks).
- Implement kernel executor scheduling/backpressure and Web host bridging.
- Fill in capability providers (Redis, Reqwest, KV SQLite) with full typestate enforcement.
- Build CLI `flows run` to execute workflows via kernel and hosts.
- Ship connector spec codegen + initial connectors (Stripe, SendGrid, Slack).
- Add importer pipeline for n8n JSON and harvesting automation.
- Integrate policy engine, registry certification, and Studio backend for agent workflows.

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
