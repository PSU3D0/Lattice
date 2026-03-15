Status: Draft
Purpose: architecture-decision / spec
Owner: Runtime
Last reviewed: 2026-03-09

# Node vs Capability Surface (0.1.x)

This document defines the boundary between:

- **graph-visible nodes** in Flow IR, and
- **capability/context API usage** inside arbitrary Rust node implementations.

This is a foundational policy for stdlib design, connector generation, and
LLM-authored flow ergonomics.

## Decision summary

Lattice is **not** a graph-only system where every capability call must become
its own node.

Lattice is also **not** an unconstrained "arbitrary Rust with ambient host
powers" system.

Instead:

- nodes are the unit of **topology, retry, validation, effect declaration,
  determinism declaration, checkpoint reasoning, and operator visibility**,
- capability/context APIs are the unit of **implementation-local resource use**
  inside a node,
- stdlib nodes are the **canonical graph-visible primitives** for operations
  that are worth reifying in topology,
- custom Rust nodes remain a first-class authoring surface, but capability use
  inside them must stay inside the existing effect/determinism/resource-hint
  contract.

## Why this decision exists

A pure node-only model becomes awkward at scale:
- graphs bloat with plumbing,
- diffs become noisy,
- retries happen at the wrong granularity,
- artifact-heavy flows become filesystem traces instead of workflow structure.

A pure context-only model becomes too loose:
- IR stops reflecting meaningful external behavior,
- host gating becomes harder to reason about,
- validator guarantees become less trustworthy,
- generators lose stable, portable targets.

The chosen model preserves the strengths of both:
- **arbitrary Rust** remains a power feature,
- **graph-visible nodes** remain the portability and governance boundary.

## Terminology

### Semantic boundary
A boundary that should be visible in topology because it represents a meaningful
step in orchestration rather than an implementation detail.

Examples:
- publish a durable artifact,
- wait for a callback,
- enumerate a workspace prefix and fan out,
- invoke an external connector operation,
- read a named artifact whose presence/absence drives branch behavior.

### Implementation-local capability use
A capability interaction that is part of how a node does its job, but is not
itself worth exposing as a graph step.

Examples:
- writing several scratch files while performing one semantic transform,
- reading an internal manifest before computing a final output,
- rearranging temporary workspace paths inside one semantic stage.

## Primary rule

A **node** should represent a **semantic orchestration unit**, not every low-level
resource operation.

A capability call should become a graph-visible node only when doing so improves:
- topology clarity,
- retry semantics,
- reuse,
- generator determinism,
- operator visibility,
- or portability.

## Criteria for graph-visible node candidacy

An operation is a good stdlib-node or generated-node candidate when several of
these are true:

1. **Topology significance**
   - the operation shapes downstream graph structure,
   - e.g. `list(prefix)` feeding fanout.

2. **Independent retry value**
   - retrying the operation separately is operationally meaningful.

3. **Generator frequency**
   - translators/LLMs are likely to emit it repeatedly as a stable primitive.

4. **Operator visibility**
   - operators benefit from seeing the step explicitly in logs/IR/trace.

5. **Cross-host semantic standardization**
   - the operation should look the same on native and Workers hosts.

## Criteria for capability-SDK-only use

An operation should usually remain inside arbitrary Rust when several of these
are true:

1. it is mostly plumbing,
2. it is high-churn implementation detail,
3. it is not useful as an independently composable graph step,
4. exposing it would make topology noisier rather than clearer,
5. multiple related capability calls really belong to one semantic retry unit.

## Governance rule for arbitrary Rust

Arbitrary Rust remains a supported authoring surface.

However, custom Rust nodes do **not** bypass governance:
- they still declare `effects`,
- they still declare `determinism`,
- they still participate in idempotency/durability validation,
- they still rely on capability/resource hints for host preflight.

In other words:
- arbitrary Rust is allowed,
- arbitrary **undeclared** host interaction is not the intended model.

## Relationship to existing capability hints

Capability/context API usage is expected to stay compatible with the existing
resource-hint model:
- `resource::*` hints remain the portability boundary,
- hosts gate flows by declared capability requirements,
- nodes that use a capability internally should still surface the correct
  effect/determinism/hint semantics at the node boundary.

This document does **not** require every capability call to be statically
enumerated in the IR. It requires the node boundary to remain truthful.

## Workspace-specific guidance

Workspace is the clearest case for this policy.

### Canonical graph-visible workspace primitives
The stdlib may expose canonical nodes such as:
- `std.workspace.read`
- `std.workspace.write`
- `std.workspace.list`
- `std.workspace.delete`

These exist because they are:
- portable,
- generator-friendly,
- easy to reason about,
- often topology-significant in artifact-heavy flows.

### What this does **not** mean
It does **not** mean every workspace interaction should be reified as its own
node.

A custom node may legitimately:
- write several scratch artifacts,
- read one back,
- delete or rename another,
- and still be one semantic node.

### Workspace examples
Good graph-visible node uses:
- list a prefix and iterate downstream,
- write a named intermediate intended for downstream consumption,
- read an artifact whose existence determines the next branch.

Good implementation-local uses:
- temporary scratch writes inside one transform node,
- small helper manifests internal to one semantic stage,
- delete/rewrite churn that is not meaningful to topology.

### `stat` / `exists`
`stat` is a useful operation, but it is **not automatically** a required stdlib
node just because the SDK exposes it.

Default posture:
- prefer SDK/helper availability first,
- promote to a stdlib node only if real graph patterns show it is frequently
  topology-significant.

### `list`
`list(prefix)` is a stronger stdlib candidate than `stat` because it often
changes the next shape of the graph and is frequently a fanout boundary.

## Stdlib guidance

Stdlib should remain small and foundational.

Implication of this decision:
- stdlib should provide **canonical graph-visible primitives**,
- stdlib should **not** try to mirror every helper available from the capability
  SDK,
- stdlib should avoid turning capabilities into fake host-filesystem or general
  scripting APIs.

## Connector and generator guidance

### Connector crates
Connector crates should usually expose **semantic nodes**, not capability micro-ops.

Example:
- `connector_google_drive.files_download`
  - good semantic node
- raw HTTP/cache/workspace calls used internally by that node
  - good implementation detail

### Translators / generators
Generators should choose between:
1. stdlib node emission when the operation is topology-significant, and
2. generated custom/helper nodes when multiple capability interactions belong to
   one semantic step.

Generators should avoid exploding flows into a long chain of capability
micro-operations unless that structure is genuinely meaningful.

## Validation implications

This policy keeps compile-time guarantees realistic:
- Flow IR remains the source of truth for topology and declared node semantics,
- capability hints remain the host preflight contract,
- arbitrary Rust remains possible inside nodes,
- but the platform does not pretend that every internal implementation detail is
  statically visible in the graph.

This is an intentional trade:
- **strong declared node contracts** over
- impossible full static introspection of arbitrary Rust internals.

## Consequences for `LAT-000026`

`LAT-000026` should be interpreted as:
- defining the canonical graph-visible workspace primitives,
- not forcing all workspace use through graph nodes,
- and not committing the platform to a node-per-filesystem-operation philosophy.

## Follow-on considerations

Potential future follow-on work:
- improve capability declaration ergonomics for custom nodes,
- add lints or helper annotations for capability-heavy nodes,
- add a separate decision note if we later want finer-grained static declaration
  of in-node capability use,
- formalize reusable connector-operation declarations for custom nodes and thin
  connector-node wrappers.

That connector-specific follow-on is now further developed in:
- `impl-docs/spec/connector-op-reuse-and-node-declaration.md`

## Cross references

- `impl-docs/spec/stdlib-and-node-registry.md`
- `impl-docs/spec/capabilities-and-binding.md`
- `impl-docs/spec/connector-op-reuse-and-node-declaration.md`
- `impl-docs/spec/workspace-capability.md`
- `impl-docs/spec/connector-and-plugin-model.md`
- `impl-docs/spec/typed-boundary-policy.md`
