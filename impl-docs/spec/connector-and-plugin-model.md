Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2026-01-29

# Connector and Plugin Model (0.1.x)

This document defines how connectors are packaged, validated, and linked into latticeflow.
Connectors are **nodes** built on top of capabilities (HTTP, KV, DB, etc.), not capabilities themselves.

For the finer-grained connector family decomposition — action nodes, polling
triggers, webhook triggers, credential roles, and activation lifecycle shapes —
see:
- `impl-docs/spec/connector-crate-surface.md`
- `impl-docs/spec/connector-trigger-runtime-contract.md`

## Goals

- Compile-time linked connectors with strong validation.
- Support sandboxed implementations (WASM) without runtime discovery.
- Provide a path to a connector registry and certification harness.

## Connector Crates

Connectors SHOULD be authored as Rust crates that expose node specs and registration helpers.

A connector crate may expose multiple semantic surface kinds:
- action nodes,
- polling triggers,
- webhook triggers,
- and shared auth/transport/lifecycle helpers.

The first runnable implementation pass may focus on action nodes, but the crate
model should not assume connectors are action-only.

Example surface:

```
connector_google::drive::files_list_node_spec()
connector_google::drive::files_list_register(&mut registry)
```

This preserves compile-time guarantees and allows dead-code elimination.

## Plugin-as-Node-Inner

When sandboxing is required, connectors may embed a WASM implementation **inside** a typed Rust node.
The node wrapper owns:

- Input/output schema enforcement.
- Effect and idempotency hints.
- Capability access (e.g., HTTP write).

The WASM module is invoked through a stable ABI (WIT or JSON bridge). The wrapper handles coercion and
errors, ensuring downstream nodes remain type-safe.

Connector implication:
- connector wrappers should generally expose **semantic nodes**, not one graph
  node per low-level capability call,
- capability interactions performed inside the wrapper remain implementation
  detail as long as the wrapper's declared effects/determinism/resource
  requirements stay truthful.

**Key rule**: plugins are resolved at build time and linked through wrapper crates; there is no dynamic
runtime discovery for production flows.

See also:
- `impl-docs/spec/node-vs-capability-surface.md`

## Manifest and Codegen

Connectors MAY ship a manifest to drive wrapper generation.

For the concrete Phase-B action-first manifest surface and generated crate
layout, see:
- `impl-docs/spec/connector-manifest-phase-b.md`

```yaml
id: connector.google.drive.files_list
inputs:
  - name: query
    type: string
outputs:
  - name: files
    type: json
effects:
  - http.read
deterministic: true
capabilities:
  - http.read
```

Codegen produces:
- Rust input/output types
- `node_spec()`
- `register()`
- ABI adapters for WASM plugin invocation

## Certification Harness

Connectors intended for registry distribution SHOULD include:

- Contract tests (error mapping, idempotency behavior)
- Effect/determinism evidence
- Capability usage declarations

Certification results are intended to be published alongside connector metadata.

## Distribution

Two supported distribution modes:

1) **Crate registry** (preferred for compile-time linking)
2) **OCI/WASM artifacts** (optional), resolved at build time into wrapper crates

Runtime-only discovery is not supported for production bundles.
