Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2025-12-12

# Flow IR (0.1.x)

This document defines the semantics of the LatticeFlow Flow IR for the 0.1.x line.

Source of truth:
- JSON Schema: `schemas/flow_ir.schema.json`
- Rust types: `crates/dag-core/src/ir.rs`

## Compatibility Rules (0.1.x)

0.1.x is intended to be "stable-ish" for toolchains (hosts, importers, exporters, UI).

- Producers MUST emit JSON that validates against `schemas/flow_ir.schema.json`.
- Consumers SHOULD ignore unknown object fields (forward-compatible).
- Within 0.1.x:
  - Additive changes MUST be optional fields with defaults.
  - Removing/renaming fields is breaking (requires 0.2.0).
  - Adding enum variants is breaking unless explicitly documented as tolerated.

## Core Concepts

- Flow: a named, versioned workflow DAG with a target execution `profile`.
- Node: a typed unit of computation (Trigger/Inline/Activity/Subflow).
- Edge: a directed connection between nodes with delivery, ordering, buffering, and timeouts.
- Control surface: declarative orchestration metadata (branching, retry, windowing, etc).
- Checkpoint: a named pause/resume boundary (used for await/external events in later epics).

## FlowIR

`FlowIR` is the top-level structure.

Key fields:
- `id`: stable UUID string (`FlowId`) derived from `(name, version)`.
- `name`: human readable flow name.
- `version`: semantic version of the flow (not the IR schema version).
- `profile`: execution target (`web|queue|temporal|wasm|dev`).
- `summary`: optional description.
- `nodes[]`: ordered list of `NodeIR`.
- `edges[]`: ordered list of `EdgeIR`.
- `control_surfaces[]`: list of `ControlSurfaceIR` (may be empty).
- `checkpoints[]`: list of `CheckpointIR` (may be empty).
- `policies`: lint/policy knobs for validation and future policy engine integration.
- `metadata`: human tags (non-semantic).
- `artifacts[]`: sidecar artifact references (DOT, schema, entrypoints manifest, etc).

## NodeIR

A node describes an executable unit.

Key fields:
- `id`: stable `NodeId` (currently derived from `flow_id` + alias; see `crates/dag-core/src/builder.rs`).
- `alias`: unique within the flow; edges reference aliases.
- `identifier`: runtime lookup key for the node implementation (registry key).
- `name`: display name.
- `kind`: `trigger|inline|activity|subflow`.
- `summary`: optional description.
- `in_schema` / `out_schema`: schema references for the node IO.
  - `opaque`: schema unknown or intentionally unconstrained.
  - `named`: a stable symbolic type name.
- `effects`: declared effects lattice (`pure|read_only|effectful`).
- `determinism`: declared determinism lattice (`strict|stable|best_effort|nondeterministic`).
- `idempotency`: optional idempotency spec (`key`, `scope`, `ttlMs`).
- `effectHints[]` / `determinismHints[]`: canonical resource hints inferred at compile-time.

Notes:
- In 0.1, schemas are primarily for validation/UX and are not enforced as structural JSON schema compatibility.
- Effect/determinism are enforced against hints by `kernel-plan`.

## EdgeIR

An edge describes how data moves between nodes.

Key fields:
- `from` / `to`: node aliases.
- `delivery`: `at_least_once|at_most_once|exactly_once`.
- `ordering`: `ordered|unordered`.
- `partition_key`: optional partition key expression (opaque string in 0.1).
- `timeout_ms`: optional per-edge timeout budget.
- `buffer`: `BufferPolicy`.

`BufferPolicy` fields:
- `max_items`: optional in-memory queue bound.
- `spill_threshold_bytes`: optional spill threshold.
- `spill_tier`: optional spill tier identifier.
- `on_drop`: optional description of drop behavior.

Notes:
- In current code, `buffer` spill behavior is implemented by `kernel-exec` (spill tiers are runtime-configured).
- `delivery=exactly_once` is validated by `kernel-plan` (dedupe + idempotency requirements).

## ControlSurfaceIR

Control surfaces describe orchestration intent.

Key fields:
- `id`: stable identifier for the surface.
- `kind`: `switch|if|loop|for_each|window|partition|timeout|rate_limit|error_handler`.
- `targets[]`: node aliases the surface references (for fast scanning).
- `config`: opaque JSON payload; 0.1 shapes are specified in `impl-docs/spec/control-surfaces.md`.

## CheckpointIR

Checkpoints declare named suspension boundaries.

Key fields:
- `id`: checkpoint name.
- `summary`: optional description.

Checkpoints become runtime-semantic once "Await/Resume" is implemented (later epic).

## IdempotencySpec

Idempotency metadata is attached to nodes.

Key fields:
- `key`: opaque string expression (canonical key derivation rule).
- `scope`: `node|edge|partition`.
- `ttlMs`: optional TTL for dedupe reservations.

In 0.1, `kernel-plan` enforces that Effectful nodes provide `idempotency.key` and that ExactlyOnce edges are backed by a dedupe capability + TTL.
