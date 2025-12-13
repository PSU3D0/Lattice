Status: Draft
Purpose: adr
Owner: Core
Last reviewed: 2025-12-12

# ADR-0002: Control Surfaces — Runtime vs Lint Scope for 0.1

## Context

Flow IR already includes control surface slots (`FlowIR.control_surfaces`) and edge-level knobs (delivery/buffer/timeout).

Implementing every orchestration surface (windowing, foreach, retries, HITL/await) before shipping any real deployment is too risky.

We need a 0.1 stance that:
- makes a small subset semantics-bearing (real runtime behavior)
- reserves the rest with stable shapes
- keeps UI/importers/agents unblocked (they can emit surfaces early)

## Decision

0.1 runtime-semantic surfaces:
- Edge-native:
  - `EdgeIR.delivery`
  - `EdgeIR.buffer` (including spill)
  - `EdgeIR.timeout_ms`
- Control-surface-native:
  - `ControlSurfaceKind::If`
  - `ControlSurfaceKind::Switch`

0.1 reserved (spec + validation, but MAY be runtime-rejected until implemented):
- `ErrorHandler` (`on_error!`)
- `RateLimit` (`rate_limit!`)
- `Partition` (`partition_by!`)
- `ForEach` (`for_each!`)
- `Window` (`window!`)
- checkpoint-based await/pause (approval/external event)

Validation behavior:
- Structural validation for reserved surfaces is allowed in 0.1.
- Runtime must fail deterministically if a reserved surface is encountered and not supported.

Config shapes are defined in `impl-docs/spec/control-surfaces.md`.

## Alternatives Considered

- Make all control surfaces lint-only in 0.1.
  - Rejected: loses the main benefit of declarative orchestration.
- Implement everything before shipping.
  - Rejected: too much surface area for 0.1.

## Consequences

- We get a real, analyzable authoring surface early.
- Importers/UI can start emitting stable control metadata now.
- Runtime complexity stays bounded for the 0.1 cycle.

## Follow-ups

- Extend `dag-macros` with the 0.1 macro primitives and populate `FlowIR.control_surfaces`.
- Teach `kernel-exec` to respect `if`/`switch` routing.
