Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2026-02-01

# WASM-First Flow Bundles (0.1.x Addendum)

This addendum defines a portable FlowBundle artifact and a WASM-first ABI for executing
flows across native hosts (wasmtime) and Workers runtimes. It extends:

- `impl-docs/spec/stdlib-and-node-registry.md` (FlowBundle + NodeRegistry concepts)
- `impl-docs/spec/subflows.md` (subflow catalog and linking rules)
- `impl-docs/spec/checkpointing-and-durability.md` (resume pinning requirements)

## Goals

- Provide a single, stable ABI for executing flow nodes in WASM.
- Define a portable FlowBundle artifact format (manifest + module) that is host-agnostic.
- Keep authoring Rust-first (macros emit IR + metadata at build time).
- Align bundle emission with the `flow!` authoring surface and `node!(...)` bindings.
- Preserve compile-time linking and validation semantics.
- Make bundle pinning explicit for checkpoint/resume safety.

## Non-goals

- Cross-host durability backends or control-plane services.
- Dynamic plugin discovery at runtime.
- Embedding secrets or credentials in bundles.
- Redefining Flow IR schemas or node semantics.

## Artifact Layout

```
flow.bundle/
  manifest.json
  manifest.sig (optional)
  module.wasm
  module.<target> (optional)
  flows/<flow_id>/flow_ir.json
  flows/<flow_id>/flow_ir_expanded.json (optional)
  subflows/<subflow_id>/flow_ir.json (optional)
  artifacts/ (optional)
```

`manifest.json` is authoritative. `module.wasm` is the compiled code bundle. Optional
artifacts may include Flow IR JSON, DOT output, or derived schemas.

## Build Matrix (0.1.x)

FlowBundle packaging supports three build shapes:

- **Single**: one flow packaged into one bundle/module.
- **Monolith**: multiple flows compiled into one bundle/module.
- **Split**: multiple bundles produced from one workspace (one bundle per flow or per shard).

Sharding is a host deployment concern and is not encoded in bundle artifacts.

Cargo metadata usage:

- Optional for single/monolith builds.
- Required for split builds to map flows to crate/module boundaries.

## Bundle Manifest (v0.1)

The manifest is the primary host-facing contract. It must be stable and self-describing.

```json
{
  "bundle_version": "0.1",
  "abi": { "name": "latticeflow.wit", "version": "0.1" },
  "bundle_id": "sha256:...",
  "code": {
    "target": "wasm32-unknown-unknown",
    "file": "module.wasm",
    "hash": "sha256:...",
    "size_bytes": 123456
  },
  "artifacts": [
    {
      "target": "x86_64-unknown-linux-gnu",
      "file": "artifacts/flow",
      "hash": "sha256:..."
    }
  ],
  "flows": [
    {
      "id": "flow://...",
      "version": "v0.1.0",
      "profile": "wasm",
      "flow_ir": { "artifact": "flows/echo/flow_ir.json", "hash": "sha256:..." },
      "flow_ir_expanded": {
        "artifact": "flows/echo/flow_ir_expanded.json",
        "hash": "sha256:..."
      },
      "entrypoints": [
        {
          "trigger": "trigger",
          "capture": "responder",
          "route_aliases": ["/echo"],
          "deadline_ms": 250
        }
      ],
      "nodes": {
        "std.timer.wait": {
          "id": "node://...",
          "effects": "effectful",
          "determinism": "nondeterministic",
          "durability": { "checkpointable": true, "replayable": true, "halts": true },
          "bindings": ["resume_scheduler"],
          "input_schema": "...",
          "output_schema": "..."
        }
      },
      "capabilities": {
        "required": [
          { "name": "resume_scheduler", "kind": "scheduler", "constraints": { "resolution_ms": 1000 } }
        ],
        "optional": []
      },
      "subflows": ["subflow.alpha@v1.0.0"]
    }
  ],
  "subflows": [
    {
      "id": "subflow.alpha",
      "version": "v1.0.0",
      "entrypoints": [],
      "effects": "effectful",
      "determinism": "nondeterministic",
      "durability": { "checkpointable": true, "replayable": true, "halts": false },
      "flow_ir": { "artifact": "subflows/alpha/flow_ir.json", "hash": "sha256:..." }
    }
  ],
  "default_flow": "flow://...",
  "signing": { "algorithm": "ed25519", "key_id": "...", "signed_at": "..." }
}
```

Notes:

- `bundle_id` is derived from a canonical JSON form of the manifest. Canonicalization:
  - Remove `bundle_id` and `signing`.
  - Omit optional fields when they are `None` (for example `flow_ir`, `flow_ir_expanded`,
    `subflows`, `signing`, and empty `artifacts`).
  - Always include empty defaults for `flows[].entrypoints`, `flows[].nodes`, and
    `flows[].capabilities` (empty arrays or empty objects) when those sections are missing.
  - Sort object keys lexicographically.
  - Sort arrays for `artifacts`, `flows`, `subflows`, and `flows[].subflows`; preserve other
    array order.
  Hosts use it for caching and resume pinning.
- The bundling CLI requires an explicit `manifest.json` sidecar; embedded WASM custom sections
  are not required for bundling.
- `code.target` declares the default execution target; `code.file` is the primary bundle artifact.
- `artifacts` is optional and carries additional target-specific artifacts with their hashes.
- `flows[].flow_ir` and `flows[].flow_ir_expanded` are optional file references; hosts should verify
  hashes when they are present.
- `entrypoints` are explicit per flow; `route_aliases` are optional and non-authoritative.
- `route_aliases` is the only alias field; `route_alias` is invalid in 0.1.x.
- Entrypoint objects are closed; unknown fields are rejected by the schema.
- `flows[].capabilities.required` declares binding names and capability kinds; credentials never
  live here.
- `flows[].capabilities.*.constraints` is an object payload (no string forms).
- `flows[].nodes.*.effects` and `flows[].nodes.*.determinism` use the Flow IR enums.
- `subflows` is a catalog of subflow descriptors; each flow lists referenced subflow ids.
- `subflows[].flow_ir` is optional and intended for analysis/UX (not execution).
- `flows[].capabilities.required`/`flows[].capabilities.optional` default to empty lists when
  omitted.
- `flows[].nodes.*.bindings` defaults to an empty list when omitted.
- All `sha256:` hashes use lowercase hex, matching the schema `[0-9a-f]{64}`.

## Multi-target Emission (CLI)

Bundle emitters MAY include multiple target artifacts in a single manifest. CLI flags:

- `--wasm` emits a WASM artifact and records it under `code` (or `artifacts` when non-default).
- `--native` emits a native artifact for the host target triple.
- `--target <triple>` emits a native artifact for the specified target triple.

When both WASM and native are emitted, the WASM artifact remains the default `code` entry, and
native artifacts are appended to `artifacts`.

## Host Selection Policy

Hosts select an execution artifact via `exec` policy:

- `exec=auto` selects the first compatible artifact, preferring native over WASM.
- `exec=native` requires a compatible native artifact; otherwise execution fails.
- `exec=wasm` requires a WASM artifact; otherwise execution fails.

Compatibility rules:

- Native artifacts are compatible only when the host target triple matches `artifacts[*].target`.
- WASM artifacts are compatible when `code.target` (or a WASM entry in `artifacts`) is a WASM target.
- If no compatible artifact exists for the selected policy, hosts must fail deterministically.

## ABI (WIT / Component Model)

The bundle must export a minimal, stable interface:

- `bundle_manifest() -> string` (JSON manifest)
- `invoke_node(alias: string, input: list<u8>) -> node_output`

`node_output` is a variant:

- `value(list<u8>)`
- `halt(list<u8>)`
- `none`
- `stream(handle)` (optional in v0.1; can be disabled)

The bundle imports host capability interfaces, such as:

- `capability.scheduler.schedule_at(handle_id, epoch_ms)`
- `capability.signal.create_token(handle_id, config) -> token`
- `capability.kv.get/put`
- `capability.blob.put/get`
- `invocation.metadata()`

All effects must go through capability imports. Direct host calls are disallowed.

## Capability Bindings

- Bundles declare logical binding names (e.g., `"customer_store"`).
- Hosts map binding names to concrete resources (KV namespaces, DB URIs, DOs, etc.).
- Credentials are host-owned and rotatable without changing bundles.
- Binding identities are pinned per `bundle_id` for resume consistency.

## Checkpoint / Resume Pinning

- Checkpoint records MUST include `bundle_id` and `flow_version`.
- Resume MUST load the exact `bundle_id`. If missing, resume fails deterministically.
- Hosts must retain old bundles until checkpoints referencing them are drained or expired.

## Subflow Linking

Subflows follow the rules in `impl-docs/spec/subflows.md` and are referenced via `subflows.entries`:

- `embedded`: subflow compiled into the same module (single bundle).
- `external`: subflow compiled as its own bundle and resolved by host catalog.

Hosts may choose embedded or external packaging strategies, but the manifest must record which
strategy was used.

## Routing + Sharding

- Default routes are derived from bundle identifiers (e.g., `/.lf/<bundle_id>/<trigger_alias>`).
- `route_aliases` are friendly overrides and are not authoritative.
- Aliases MAY be overridden by host config for public-facing routes.
- Sharding is a host deployment concern and is not encoded in bundles.

## Build Profiles

- Development builds favor fast iteration and omit `wasm-opt`.
- Release builds may apply `wasm-opt` and size checks.
- The manifest must always reflect the final code hash.

## Security and Limits

Hosts should enforce:

- memory limits and fuel/timeout budgets per invocation
- capability call limits (rate and count)
- size limits on payloads and blobs

Capability calls should be logged to a structured ledger for debugging and policy enforcement.
