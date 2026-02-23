Status: Draft
Purpose: spec
Owner: Runtime
Last reviewed: 2026-02-22

# Typed Boundary Policy (0.1.x)

This policy defines when bare JSON (`serde_json::Value` / `JsonValue`) is allowed in node APIs.

## Rule

Internal nodes should be strongly typed.

Bare JSON input+output (`Value -> Value`) is only permitted when the node is explicitly
annotated as a boundary bridge.

## Why

Without guardrails, flows can appear strongly typed at authoring time while gradually degrading into
unconstrained JSON passthrough internals.

## Escape hatch

Use explicit annotation on node definitions:

```rust
#[def_node(
  summary = "Boundary bridge",
  effects = "Pure",
  determinism = "Strict",
  json_boundary = true
)]
```

This emits an IR-visible hint:
- `policy::json_boundary`

## Validation behavior

`kernel-plan` rejects internal nodes that:
- have inbound and outbound edges,
- use bare JSON schema for both input and output,
- and do not include `policy::json_boundary`.

Diagnostic code:
- `TYPE001`

## Non-goals

- This policy does not ban JSON at ingress/egress boundaries.
- This policy does not replace capability/effect determinism checks.
