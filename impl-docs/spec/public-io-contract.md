Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2026-02-22

# Public I/O Contract for Flows and Subflows (0.1.x)

This document defines the generated public type surfaces emitted by `flow!` and `workflow!`.

## Goals

- Provide deterministic, importable type paths for reusable flows/subflows.
- Preserve strong typing with explicit adapter pathways.
- Avoid implicit shape compatibility.

## Generated contract surfaces

### Entrypoint contract surface (`flow!`)

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

Semantics:

- `RawIn` / `RawOut`: canonical boundary types derived from trigger/capture node bindings.
- `In` / `Out`: nominal wrapper types around `RawIn` / `RawOut` for explicit adapter pathways.
- `ENTRY`: typed entrypoint constant (`FlowEntrypoint<RawIn, RawOut>`).
- `CONTRACT_ID`: deterministic identifier derived from `flow_name`, `flow_version`, trigger alias, and capture alias.

### Binding contract surface (`flow!` and `workflow!`)

For each `let <alias> = node!(...)` or `let <alias> = subflow!(...)` binding, macros emit a binding module:

```rust
<flow_name>::bindings::<alias>::RawIn
<flow_name>::bindings::<alias>::RawOut
<flow_name>::bindings::<alias>::In
<flow_name>::bindings::<alias>::Out
<flow_name>::bindings::<alias>::BINDING_ALIAS
<flow_name>::bindings::<alias>::SOURCE_KIND
```

For subflow bindings, macros also emit:

```rust
<flow_name>::bindings::<alias>::SOURCE_CONTRACT_ID
```

`SOURCE_KIND` values are currently:

- `"node"`
- `"subflow"`

`SOURCE_CONTRACT_ID` is present only for `subflow!` bindings and resolves to the selected subflow entrypoint contract ID.

### Wrapper properties

`In` and `Out` wrappers are generated as transparent tuple wrappers on both entrypoint and binding surfaces:

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

3. Implement conversions against alias bindings for composition-local adapters:

```rust
impl From<ParentPayload> for parent_flow::bindings::sub::In { ... }
```

Implicit runtime shape-matching is out-of-policy.

## Determinism

Contract paths and identifiers are deterministic:

- module paths are derived from macro literals (`flow_name`, `entrypoint` aliases, binding aliases),
- IDs are derived from literals available to macro expansion,
- no runtime-generated names.

## Compatibility and versioning

Breaking changes include:

- changing `RawIn`/`RawOut` underlying types,
- renaming trigger/capture aliases for exported entrypoints,
- renaming binding aliases used by exported `bindings::<alias>` modules,
- changing flow version without coordinated contract migration.

Non-breaking additions:

- adding new entrypoints,
- adding new bindings,
- adding new helper docs/constants without changing existing type paths.
