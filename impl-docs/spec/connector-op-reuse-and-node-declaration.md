Status: Draft
Purpose: architecture-decision / spec
Owner: Runtime
Last reviewed: 2026-03-15

# Connector Operation Reuse and Node Declaration (0.1.x)

This document defines how connector crates should support **both**:

- canonical graph-visible connector nodes, and
- typed connector usage inside custom Rust nodes.

It extends:
- `impl-docs/spec/node-vs-capability-surface.md`
- `impl-docs/spec/connector-crate-surface.md`
- `impl-docs/spec/capabilities-and-binding.md`
- `impl-docs/spec/connector-connection-bindings.md`

## Problem

We want to avoid two bad extremes:

1. **node-only connectors**
   - every connector interaction must become graph topology
   - causes graph bloat and the wrong retry granularity

2. **ambient connector powers**
   - arbitrary Rust nodes silently reach into host-owned connector runtime
   - weakens deployment validation and operator trust

We need a model where connector implementations are written once, reusable from
custom nodes, and still visible to validation/deployment machinery.

## Decision summary

Connector crates should expose **three conceptual layers**:

1. **operation layer**
   - typed reusable semantic connector operations
   - intended for custom Rust nodes and codegen reuse

2. **canonical node layer**
   - thin graph-visible wrappers over those operations
   - intended for generators/LLMs/simple composition

3. **runtime/binding layer**
   - host-owned auth/profile/provider resolution
   - shared by both operation and node layers

Custom nodes may use connector operations internally, but they must declare
those operations at the node boundary.

## Primary rule

A connector action should be implemented **once** and then reused in two ways:

- as a canonical graph-visible connector node
- as a typed operation callable inside a custom node

The node wrapper must not own the core implementation.

## Why this is the right boundary

This matches the existing node-vs-capability philosophy:

- graph-visible nodes remain the unit of topology, retry, visibility, and
  portability
- custom Rust remains first-class for composition-local semantics
- host-owned connector bindings remain explicit and governable

It also preserves a strong farming path:

- generators can emit canonical nodes
- expert authors can collapse multiple connector calls into one semantic step
- both paths reuse the same connector runtime and bindings

## Proposed connector crate surface

## 1. Operation layer

Every semantic connector action should have a reusable operation type.

Conceptual generated shape:

```rust
pub mod ops {
    pub struct AppendRow;

    impl AppendRow {
        pub const META: ::dag_core::ConnectorOpMetadata = ...;

        pub async fn invoke(
            input: &AppendRowInput,
        ) -> Result<AppendRowOutput, connector_google_sheets::runtime::errors::ConnectorRuntimeError> {
            ...
        }
    }
}
```

Properties:
- `invoke(...)` is the reusable implementation surface
- `META` is the declarative contract surface used by macros/builders/validators
- operation types are zero-sized and stable to reference in macros

## 2. Canonical node layer

Canonical connector nodes become thin wrappers over operations.

Conceptual shape:

```rust
#[def_node(
    name = "GoogleSheetsAppendRow",
    identifier = "connector.google.sheets.append_row",
    connector_ops(connector_google_sheets::ops::AppendRow)
)]
pub async fn google_sheets_append_row(
    input: AppendRowInput,
) -> NodeResult<AppendRowOutput> {
    connector_google_sheets::ops::AppendRow::invoke(&input)
        .await
        .map_err(...)
}
```

Important consequence:
- the graph-visible node does **not** own the implementation
- codegen and custom Rust both reuse the same semantic operation

## 3. Optional family client layer (later)

A future ergonomic layer may provide family-scoped helpers, for example:

```rust
let sheets = connector_google_sheets::client_from_current()?;
sheets.append_row(req).await?;
sheets.get_values(req2).await?;
```

This is useful, but it should be layered **on top of** operation metadata, not
instead of it.

For 0.1.x, operation types are sufficient.

## New metadata type

Add a reusable metadata type in `dag-core` (or equivalent neutral contract home)
so macros can reference it without requiring proc-macro evaluation of foreign
constants.

Conceptual shape:

```rust
pub struct ConnectorOpMetadata {
    pub operation_id: &'static str,
    pub connector_id: &'static str,
    pub summary: &'static str,

    // semantic envelope
    pub min_effects: Effects,
    pub max_determinism: Determinism,

    // raw portability hints
    pub effect_hints: &'static [&'static str],
    pub determinism_hints: &'static [&'static str],

    // connector binding/runtime needs
    pub roles: &'static [ConnectorRoleRequirement],
}

pub struct ConnectorRoleRequirement {
    pub kind: ConnectorRoleKindDecl,
    pub name: &'static str,
    pub expected_handle_kind: &'static str,
}

pub enum ConnectorRoleKindDecl {
    OutboundAuth,
    ProvisioningAuth,
    InboundVerifier,
    EndpointProfile,
}
```

Notes:
- `min_effects` and `max_determinism` define the operation envelope
- raw `effect_hints` / `determinism_hints` preserve compatibility with existing
  validator logic
- `roles[]` becomes the structured deployment/binding validation surface

## `def_node` extension

Add a new attribute section:

```rust
#[def_node(
    ...,
    connector_ops(
        connector_google_sheets::ops::AppendRow,
        connector_google_sheets::ops::GetValues,
    )
)]
```

### Semantics

Each path refers to an operation type with a `META` constant:

```rust
connector_google_sheets::ops::AppendRow::META
```

The macro should emit those references into `NodeSpec` without trying to fully
interpret them during proc-macro expansion.

## `NodeSpec` extension

Extend `NodeSpec` with:

```rust
pub struct NodeSpec {
    ...
    pub connector_ops: &'static [&'static ConnectorOpMetadata],

    // whether the author explicitly declared these fields
    pub effects_declared: bool,
    pub determinism_declared: bool,
}
```

Why the declared flags matter:
- if effects/determinism were omitted, connector operations should be allowed to
  auto-hoist the node envelope
- if effects/determinism were explicitly declared, contradictions should fail

## `NodeIR` extension

Add structured connector dependency emission to Flow IR.

Conceptual shape:

```rust
pub struct NodeIR {
    ...
    #[serde(rename = "connectorOps", default)]
    pub connector_ops: Vec<ConnectorOpRefIR>,
}

pub struct ConnectorOpRefIR {
    pub operation_id: String,
    pub connector_id: String,
    pub roles: Vec<ConnectorRoleRequirementIR>,
}
```

This gives deployment tooling something stronger than raw `resource::http::*`
hints.

## Builder / macro behavior

## Raw hint hoisting

When `connector_ops(...)` is present:
- merge all operation `effect_hints`
- merge all operation `determinism_hints`
- preserve de-duplication semantics already used for resource hints

This means authors do **not** need to manually restate low-level resource
requirements like `http_write` just because they declared a connector operation.

## Effects hoisting

If `effects_declared == false`:
- set node effects to the maximum of all declared operation `min_effects`
- if no operations are present, keep current default behavior

If `effects_declared == true`:
- preserve the declared value
- let validation fail if it is weaker than the required envelope

## Determinism hoisting

If `determinism_declared == false`:
- set node determinism to the weakest/least-strict value required by the
  declared operations

If `determinism_declared == true`:
- preserve the declared value
- let validation fail if it is stricter than allowed by the required envelope

## Validation model

### What should be automatic

Declaring connector operations should automatically hoist:
- raw resource hints (for example `resource::http::write`)
- effect envelope
- determinism envelope
- structured connector dependency metadata

### What should remain explicit

Do **not** auto-infer node idempotency policy.

Reason:
- a custom node may call a connector operation conditionally
- wrapper logic may add or remove dedupe behavior
- idempotency remains a node-boundary semantic contract

So the node author still owns:
- idempotency key
- dedupe TTL
- exactly-once semantics where relevant

## Resource declaration policy

## Preferred policy

When a node declares connector operations, authors should **not** have to also
manually declare raw `resources(http_write(...))` purely for those operations.

The connector operation declaration is the semantic abstraction boundary, so the
system should derive lower-level resource needs automatically.

### Example

Good:

```rust
#[def_node(
    name = "MaybeAppendRow",
    connector_ops(connector_google_sheets::ops::AppendRow),
    effects = "Effectful",
    determinism = "BestEffort"
)]
```

Unnecessary repetition:

```rust
#[def_node(
    ...,
    connector_ops(connector_google_sheets::ops::AppendRow),
    resources(http_write(capabilities::http::HttpWrite))
)]
```

The second form should remain allowed if the node also uses extra raw HTTP
outside the connector operation, but it should not be required.

## Why this is better for agent authoring

Agents and humans think in terms of:
- "this node may append a Sheets row"

not:
- "this implies http write, endpoint role, outbound auth, effectful,
  best-effort..."

The machine should hoist the lower-level envelope.

## Operation declaration granularity

## Preferred first step: operation-level declarations

Use specific operations, not broad family grants, for the initial design.

Why:
- preserves the strongest static envelope
- avoids over-granting roles/hints
- gives generators stable reusable units
- keeps deployment validation precise

Example:

```rust
connector_ops(
    connector_google_sheets::ops::AppendRow,
    connector_google_sheets::ops::GetValues,
)
```

## Family-level declarations (later, optional)

A broader declaration like:

```rust
connector_family("connector.google.sheets")
```

may be useful later for highly dynamic custom nodes, but it is intentionally
more permissive and should be deferred.

## Enforcement model

## 0.1.x practical stance

The platform should require connector operation declarations by policy, but it
should not block the first design on perfect introspection of arbitrary Rust
bodies.

That means:
- declared operations are authoritative for governance/validation
- helper APIs may still read from current connector runtime/context internally
- undeclared connector use is initially a lint/policy smell rather than
  something we promise to catch perfectly at the Rust-body level

## Future tightening path

If stronger enforcement becomes important later, a typed/witness-based helper
surface can be added so connector operations are only obtainable through a
macro-provided declaration context.

That should be treated as a later hardening step, not a prerequisite.

## Generated connector crate reorganization

Current generated crates already have:
- descriptors
- runtime transport
- thin action nodes

Proposed additive structure:

```text
src/
  lib.rs
  ext.rs
  runtime/
  generated/
    actions/     # canonical graph-visible node wrappers
    ops/         # reusable semantic connector operations
    profiles/
    types/
    register/
```

Public exports:

```rust
pub mod ops {
    pub use crate::generated::ops::*;
}

pub use generated::actions::*;
```

Migration-friendly consequence:
- existing canonical node paths remain usable
- new custom-node operation reuse becomes first-class

## Example: `MaybeAppendRow`

```rust
#[def_node(
    name = "MaybeAppendRow",
    summary = "Append a row only when the lead qualifies",
    connector_ops(
        connector_google_sheets::ops::AppendRow
    )
)]
async fn maybe_append_row(input: LeadAnalysis) -> NodeResult<AppendOutcome> {
    if !input.should_append {
        return Ok(AppendOutcome { appended: false });
    }

    let req = connector_google_sheets::AppendRowInput::from(input);
    connector_google_sheets::ops::AppendRow::invoke(&req)
        .await
        .map_err(|err| NodeError::new(err.to_string()))?;

    Ok(AppendOutcome { appended: true })
}
```

Semantics:
- topology sees one semantic node
- implementation reuses the canonical append-row connector operation
- raw HTTP write / connector binding requirements are hoisted automatically
- idempotency policy remains explicit at the node boundary

## Immediate implementation sequence

1. Introduce `ConnectorOpMetadata` and `NodeSpec.connector_ops`
2. Generate reusable operation types under `generated/ops/`
3. Rewrite generated action nodes as thin wrappers over operations
4. Add `def_node(connector_ops(...))`
5. Auto-hoist hints/effect/determinism envelopes from declared operations
6. Emit structured connector operation metadata into `NodeIR`
7. Add deployment validation that consumes `NodeIR.connector_ops[]`

## Guidance for agent authoring

When writing custom nodes:

1. If the external action is topology-significant, use the canonical connector
   node directly.
2. If the connector call is part of a larger semantic step, declare the
   connector operation and call it inside the node.
3. Do not manually restate low-level raw resources solely for declared
   connector operations.
4. Still declare truthful node-level idempotency semantics.

## Cross references

- `impl-docs/spec/node-vs-capability-surface.md`
- `impl-docs/spec/connector-crate-surface.md`
- `impl-docs/spec/capabilities-and-binding.md`
- `impl-docs/spec/connector-connection-bindings.md`
