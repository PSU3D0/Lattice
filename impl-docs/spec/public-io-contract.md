Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2026-02-22

# Public I/O Contract for Flows and Subflows (0.1.x)

This document defines the generated public type surface emitted by `flow!` for entrypoints.

## Goals

- Provide deterministic, importable type paths for reusable flows/subflows.
- Preserve strong typing with explicit adapter pathways.
- Avoid implicit shape compatibility.

## Generated contract surface

For each `entrypoint!` in `flow!`, macros emit a contract module:

```rust
<flow_name>::contract::<trigger_alias>::RawIn
<flow_name>::contract::<trigger_alias>::RawOut
<flow_name>::contract::<trigger_alias>::In
<flow_name>::contract::<trigger_alias>::Out
<flow_name>::contract::<trigger_alias>::ENTRY
<flow_name>::contract::<trigger_alias>::FLOW_NAME
<flow_name>::contract::<trigger_alias>::FLOW_VERSION
<flow_name>::contract::<trigger_alias>::CONTRACT_ID
```

### Semantics

- `RawIn` / `RawOut`: canonical boundary types derived from trigger/capture node bindings.
- `In` / `Out`: nominal wrapper types around `RawIn` / `RawOut` for explicit adapter pathways.
- `ENTRY`: typed entrypoint constant (`FlowEntrypoint<RawIn, RawOut>`).
- `CONTRACT_ID`: deterministic identifier derived from `flow_name`, `flow_version`, trigger alias, and capture alias.

### Wrapper properties

`In` and `Out` are generated as transparent tuple wrappers:

```rust
#[repr(transparent)]
pub struct In(pub RawIn);

#[repr(transparent)]
pub struct Out(pub RawOut);
```

Macros also emit `From` conversions:
- `RawIn -> In`
- `In -> RawIn`
- `RawOut -> Out`
- `Out -> RawOut`

This keeps adapter pathways explicit while staying layout-clean.

## Adapter policy

Compatibility must be explicit. Preferred patterns:

1. Implement conversion in code:
```rust
impl From<ExternalPayload> for child_flow::contract::trigger::In { ... }
```

2. Use adapter nodes that perform explicit conversion.

Implicit runtime shape-matching is out-of-policy.

## Determinism

Contract paths and identifiers are deterministic:
- module path based on flow + trigger alias,
- IDs based on literals available to `flow!` expansion,
- no runtime-generated names.

## Compatibility and versioning

Breaking changes include:
- changing `RawIn`/`RawOut` underlying types,
- changing trigger/capture aliases for exported entrypoints,
- changing flow version without coordinated contract migration.

Non-breaking additions:
- adding new entrypoints,
- adding new helper docs/constants without changing existing type paths.
