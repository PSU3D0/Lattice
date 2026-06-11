Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2026-06-11

# Flow Requirements Manifest (0.1)

This document specifies `FlowRequirements`: a static, machine-readable
manifest that answers "what does this flow need to run?" entirely from the
bundle, executing nothing.

It is the seed artifact for infra-from-code (workstream C of the 2026-06-10
verifiability plan): an infrastructure planner must be able to read a flow
bundle and decide placement — Cloudflare Workers free tier, paid tier, native
host — from declared metadata alone. This manifest carries the *declared*
half of the answer in the bundle and draws a precise boundary around the half
that belongs to bindings.lock generation (since packet C2, host preflight
reads both halves as data and performs no runtime resolution).

Related docs:
- `impl-docs/spec/flow-bundles-wasm.md` (bundle manifest the requirements ride in)
- `impl-docs/spec/capabilities-and-binding.md` (effect hints, enforcement semantics)
- `impl-docs/spec/connector-connection-bindings.md` (connection instances, role bindings)
- `impl-docs/spec/connector-op-reuse-and-node-declaration.md` (`ConnectorOpMetadata`, resolution modes)
- `impl-docs/spec/checkpointing-and-durability.md` (durability services preflight demands)
- `schemas/flow_requirements.schema.json` (generated JSON schema)

## Goals

- Every flow requirement answerable statically from `ValidatedIR` plus the
  connector operation metadata already serialized into it.
- Per-flow union for planners, per-node attribution for debuggers.
- A stable, versioned JSON shape carried inside the bundle manifest.
- An explicit manifest-time vs lock-time boundary so packet C2 can move
  runtime preflight resolution into bindings.lock generation without changing
  this shape.

## Non-goals

- Instance binding (which KV namespace, which connection secret): bindings.lock.
- Runtime capability enforcement (packet A2's `ScopedResources` owns that).
- Resource sizing estimation (CPU/memory profiles); only structural
  requirements are recorded.

## The static derivability rule

Every field of `FlowRequirements` MUST be computable from:

1. a `ValidatedIR` (kernel-plan validated Flow IR), and
2. the connector operation metadata already hoisted into it
   (`NodeIR.connector_ops`, populated from `ConnectorOpMetadata` at macro
   expansion), and
3. optionally, bundle-assembly context (the serialized Flow IR hash and flow
   registry entrypoint specs).

Nothing may require executing a node, calling a live connector runtime,
reading the environment, or consulting a deployment. `derive_requirements`
is a pure function; the C2 regression test (preflight performs zero
`ConnectorRuntime` calls) extends the same guarantee to preflight.

## Manifest shape (schema_version 0.1)

Top-level type: `dag_core::requirements::FlowRequirements`
(serde + schemars; JSON schema generated at
`schemas/flow_requirements.schema.json` via
`cargo run -p dag-core --bin emit_schemas -- flow_requirements.schema.json`).

Example (the s12 SheetPort bound flow as carried in a bundle; the IR-derived
golden fixture at
`crates/kernel-plan/tests/fixtures/s12_sheetport_quote_bound.requirements.json`
is identical minus the two bundle-assembly enrichments, `deadline_ms` and
`flow_ir_hash`):

```json
{
  "schema_version": "0.1",
  "flow": {
    "id": "bac0586b-907c-5d76-8f31-029beefc2977",
    "name": "s12_sheetport_quote_flow",
    "version": "1.0.0"
  },
  "profile": "web",
  "effects": {
    "union": ["resource::blob::read"],
    "families": ["resource::blob"],
    "per_node": { "evaluate": ["resource::blob::read"] }
  },
  "connectors": [
    {
      "connector_id": "connector.formualizer.sheetport",
      "operations": [
        {
          "operation_id": "connector.formualizer.sheetport.evaluate",
          "supported_resolution_modes": ["bound_connection", "late_bound_refs", "inline_payload"],
          "default_resolution_mode": "bound_connection",
          "selected_resolution_modes": ["bound_connection"],
          "requires_bound_connection": true,
          "nodes": ["evaluate"]
        }
      ]
    }
  ],
  "durability": {
    "mode": "partial",
    "has_halting_nodes": false,
    "needs_checkpoint_store": true,
    "needs_resume_scheduler": false,
    "needs_resume_signal_source": false,
    "needs_checkpoint_blob_store": false
  },
  "triggers": [
    { "alias": "trigger", "identifier": "example_s12_sheetport_quote::quote_trigger", "kind": "http" }
  ],
  "entrypoints": [
    {
      "trigger_alias": "trigger",
      "capture_alias": "capture",
      "route_path": "/quote",
      "method": "POST",
      "route_aliases": ["/quote"],
      "deadline_ms": 2000
    }
  ],
  "host": {
    "requires_wasm32_compatibility": false,
    "requires_connector_runtime": true,
    "has_subflows": false
  },
  "flow_ir_hash": "sha256:..."
}
```

## Derivation rules

| Field | Derived from | Rule |
| --- | --- | --- |
| `schema_version` | constant | `FLOW_REQUIREMENTS_SCHEMA_VERSION` (`"0.1"`). |
| `flow.{id,name,version}` | `FlowIR` | Copied verbatim; `id` is the UUIDv5 of `name:version`. |
| `profile` | `FlowIR.profile` | Copied verbatim. |
| `effects.union` | `NodeIR.effect_hints` | Union of all hints that parse as `dag_core::EffectHint`, sorted by canonical string. `policy::*` markers (e.g. the TYPE001 `policy::json_boundary` annotation) are lint metadata, not capability requirements, and are skipped. Any other unparseable hint fails derivation closed (same condition kernel-plan rejects as EFFECT202). Connector-op effect hints are already included because macro expansion hoists `ConnectorOpMetadata.effect_hints` into `NodeIR.effect_hints`. |
| `effects.families` | `effects.union` | `EffectHint::family()` of each union member, deduplicated, sorted. A planner provisioning capability providers works at this granularity. |
| `effects.per_node` | `NodeIR.effect_hints` | Node alias → sorted hints; only nodes declaring at least one capability hint appear. |
| `connectors` | `NodeIR.connector_ops` | Grouped by `connector_id`, then `operation_id`. Per operation: declared `roles` (`ConnectorOpMetadata.roles` as serialized in `ConnectorOpRefIR`), `supported_resolution_modes` and `default_resolution_mode` verbatim, `selected_resolution_modes` = sorted set of the modes nodes actually selected, `requires_bound_connection` = any selection is `bound_connection`, `nodes` = sorted aliases declaring the op. |
| `durability` | `FlowIR.policies.durability` + `NodeIR.durability` + node identifiers | Mirrors host-inproc `collect_missing_durability_services` exactly: `needs_checkpoint_store` ⇔ mode ≠ `off`; `needs_resume_scheduler` ⇔ halting nodes present AND a `std.timer.wait` node exists; `needs_resume_signal_source` ⇔ halting nodes present AND a `std.callback.wait`/`std.hitl.approval` node exists; `needs_checkpoint_blob_store` ⇔ mode ≠ `off` AND `blob_threshold_bytes` configured. |
| `triggers` | `NodeIR.kind == Trigger` + `FlowMetadata.entrypoints` | One entry per trigger node. `kind` is `http` when the trigger alias is wired to an entrypoint, else `unspecified` (the IR records no richer trigger taxonomy today; extend the enum when polling/webhook trigger runtimes land). |
| `entrypoints` | `FlowMetadata.entrypoints` (+ registry specs at bundle time) | Route path/method/aliases copied from IR metadata. `deadline_ms` is NOT in Flow IR metadata today; it is enriched during bundle assembly from the flow registry's `EntrypointSpec` (`exporters::bundle`). When derived directly from IR (e.g. future `flows bundle requirements` on a bare IR), `deadline_ms` is `null`. |
| `host.requires_wasm32_compatibility` | `FlowIR.profile` | `profile == wasm`. (All bundles ship wasm32 code today; this flags flows that can ONLY be placed on a wasm-capable host.) |
| `host.requires_connector_runtime` | `NodeIR.connector_ops` | Any op selected in `bound_connection` mode. A `ConnectorRuntime` must be bound for execution, and preflight requires the lock-recorded resolution results (`connector_bindings.<flow>.resolved_effect_hints` in bindings.lock, one entry per bound node alias); preflight itself performs no runtime resolution (C2). |
| `host.has_subflows` | `NodeIR.kind == Subflow` | Host must support subflow expansion/linking. |
| `flow_ir_hash` | bundle assembly | `sha256:<hex>` of the serialized `flow_ir.json` artifact; `null` when derived outside a bundle. |

## Manifest-time vs lock-time boundary

This is the load-bearing design decision; C2 depends on it.

**Manifest time (this document)** records the *declared contract*:

- which connector operations a flow may invoke;
- which resolution modes each operation supports and which the flow selected;
- which auth/endpoint roles the connector crate requires
  (`outbound_auth`, `provisioning_auth`, `inbound_verifier`,
  `endpoint_profile`) and the handle kind each expects;
- which capability hints the flow's own nodes declare.

**Lock time (bindings.lock generation)** owns *instance satisfaction*:

- mapping each required role to a concrete connection instance and handle
  (`connector_connections` / `connector_handles` in bindings.lock);
- resolving the *connection-dependent* effect hints (e.g. SheetPort's bound
  connection storing its workbook in blob ⇒ `resource::blob::read`) and
  recording them per (flow, node alias) in
  `connector_bindings.<flow>.resolved_effect_hints`. These hints depend on
  which connection is bound, so they are inherently not manifest-static —
  they are *lock-static*: computed once, by `flows bindings lock generate`,
  and read as data from then on (before C2 they were re-computed at runtime
  preflight via `ConnectorRuntime::resolve_required_effect_hints`).

Note the distinction in the s12 example: the SheetPort `evaluate` op's
`ConnectorOpMetadata` declares `resource::blob::read` unconditionally, so it
appears in the manifest's `effects.union` (hoisted into `NodeIR.effect_hints`
at macro expansion). Anything a connector resolves *per connection* beyond
its static declaration is lock-time data and intentionally absent here.

**Runtime (since C2)** performs no resolution: preflight is a pure data
comparison of (declared hints ∪ lock-recorded hints) against the bound
resource set. The former nested-Tokio preflight resolution path in
host-inproc was deleted by packet C2; `flows bindings lock generate` derives
the connection-dependent hints and records them in
`connector_bindings.<flow>.resolved_effect_hints` (keyed by node alias —
present-but-empty means "resolved: nothing beyond static declarations",
absent means "not recorded" and bound-connection preflight fails closed
asking for lock regeneration). Hosts running flows from directly constructed
resource bags (tests, embedded hosts) preflight against statically declared
hints only; A2's scoped views still deny any unrecorded over-reach at access
time.

## Versioning policy

- `schema_version` versions the manifest *shape*. `0.1` is the initial shape.
  Additive optional fields do not bump it; renames, removals, or semantic
  changes to existing fields do. Consumers MUST reject unknown major shapes.
- `flow.{id,name,version}` pins the manifest to a flow revision. `flow.id`
  is derived from name+version, so a version bump changes the id.
- `flow_ir_hash` pins the manifest to the exact serialized IR artifact in the
  bundle, enabling drift detection between `flow_ir.json` and a stale
  manifest.
- The enclosing `bundle_id` is deliberately NOT embedded: the requirements
  manifest is part of the hashed manifest content, so embedding the bundle id
  would be circular. Consumers reading from a bundle take bundle identity
  from the enclosing `manifest.json`.
- Unknown effect-hint strings fail derivation closed (EFFECT202 semantics);
  a manifest can therefore never contain a hint outside the
  `dag_core::EffectHint` vocabulary of the toolchain that produced it.

## Carriage in the flow bundle

`flow_bundle::FlowEntry` gains an optional `requirements` field:

- Serialized only when present (`skip_serializing_if`), so pre-existing
  manifests and their `bundle_id` hashes are unaffected; back-compat is
  covered by `crates/flow-bundle/tests/requirements_carry.rs`.
- `null` is rejected (same non-null-option policy as `signing`/`flow_ir`).
- `exporters::bundle::build_manifest_from_registry` populates it for every
  flow entry, enriching `flow_ir_hash` and entrypoint `deadline_ms` — the two
  values only the bundle assembler knows.
- `schemas/flow_bundle.schema.json` admits the field as an object; its full
  shape is the sibling `flow_requirements.schema.json` (kept un-`$ref`ed to
  avoid cross-file resolution in offline validators).

## How an infra planner consumes this (forward-looking)

The future planner (`manifest → {cf-workers-free | cf-paid | native}`) reads,
per flow entry:

- `effects.families` → which capability providers to provision
  (KV namespace, R2 bucket, D1 database, outbound fetch).
- `connectors[].operations[].roles` + `requires_bound_connection` → which
  secrets/connection instances the deploy must bind before activation.
- `durability` → whether a checkpoint store (e.g. KV/D1/DO-backed) and
  resume scheduler (e.g. Workers alarms) must exist.
- `entrypoints` → routes/methods to wire (worker routes), `deadline_ms` →
  response-time budget.
- `host` → placement constraints (wasm32-only flows cannot fall back to a
  native host path).

Cloudflare sizing constraints the planner must eventually answer from the
manifest plus artifact metadata (`code.size_bytes`):

- **1 MiB compressed per script (free tier)** — bundle module size vs tier;
  multi-flow monolith bundles may need splitting.
- **100 scripts per account (free tier)** — flows-per-script packing;
  multi-flow bundles via dispatch (one dispatcher script fronting many flows
  in one module) trade script count against module size.
- **Workers for Platforms dispatch** for fleets beyond those limits.

The C3 CLI command (`flows bundle requirements`) emits exactly this JSON for
any example/bundle so the planner can be developed against goldens.

## API surface

- `dag_core::requirements::FlowRequirements::derive(&FlowIR) -> Result<FlowRequirements, RequirementsError>`
  — the core pure derivation. Lives in dag-core because every input is a
  dag-core type and both flow-bundle and exporters (which must not depend on
  kernel-plan) need it. Fails closed on non-canonical hints.
- `kernel_plan::derive_requirements(&ValidatedIR) -> FlowRequirements`
  — the public entrypoint. Infallible: EFFECT202 validation guarantees hint
  canonicality, which is exactly the failure mode of the core derivation.
  Use this unless you are the bundle exporter operating on
  registry-validated IR.
- `FlowRequirements::with_flow_ir_hash(hash)` — bundle-assembly enrichment.

## Test fixtures

Handwritten golden manifests (also the seed goldens for C3's CLI tests):

- `crates/kernel-plan/tests/fixtures/s1_echo.requirements.json`
  (pure flow: empty effects, http entrypoint, partial durability)
- `crates/kernel-plan/tests/fixtures/s12_sheetport_quote_bound.requirements.json`
  (bound-connection connector op: `requires_bound_connection: true`,
  `requires_connector_runtime: true`)
- `crates/kernel-plan/tests/fixtures/s12_sheetport_quote_internal.requirements.json`
  (same op selected as `late_bound_refs`: no bound-connection requirement)

Asserted by `crates/kernel-plan/tests/flow_requirements_golden.rs`; schema
conformance asserted by
`crates/flow-bundle/tests/requirements_carry.rs::requirements_payload_validates_against_flow_requirements_schema`.
