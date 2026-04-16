# SheetPort connector architecture notes

Status: draft / local scaffold companion

This file explains the intended architecture behind the current local scaffold
for `connector_formualizer_sheetport`.

It is intentionally more opinionated than the crate README.

## Current architectural stance

SheetPort should be treated as a **semantic connector family**, not a raw
capability.

Lower-level capabilities remain:
- blob
- workspace
- HTTP (if ever needed for remote fetch paths)

SheetPort sits above them as a connector/runtime surface that turns a workbook +
manifest into a typed semantic operation.

## Key distinction: connector usage vs source resolution

There are two separate axes here.

### Axis 1 — where the connector is used
- canonical graph-visible node
- internal reusable operation in a custom Rust node

### Axis 2 — how the workbook/manifest model is selected
- deployment-bound connection config
- late-bound typed source refs

The current scaffold is intentionally aimed at supporting both axes without
allowing ambient unbound access.

## What the current build is trying to preserve

### 1. Strong default path
The canonical/default path should be:
- node uses a deployment-bound SheetPort connection
- connection selects workbook + manifest + eval defaults

This keeps preflight and operator understanding strong.

### 2. Honest dynamic path
The internal operation surface should remain capable of supporting:
- late-bound workbook refs
- late-bound manifest refs
- inline manifest payloads

but only when those are explicit and typed.

### 3. No ambient power
This connector should not become a backdoor for:
- arbitrary file access
- arbitrary undeclared storage access
- undeclared workbook/model resolution

## Recommended resolution modes

## Mode A — bound connection (preferred default)
Deployment/runtime binds:
- workbook source
- manifest source
- eval defaults
- artifact policy

This is the flagship/canonical path for graph-visible topology.

## Mode B — late-bound typed refs (advanced)
Invocation supplies source refs such as:
- blob object key for workbook
- blob object key for manifest
- inline manifest YAML

This is useful for dynamic and user-driven scenarios, especially inside custom
nodes, but should remain explicit.

## Mode C — direct raw object payloads (selective only)
Potential later support:
- inline workbook bytes
- direct uploaded artifacts

This is not the design center for the current scaffold.

## Why the current scaffold still centers bound mode

Bound mode remains the best default because it maximizes:
- preflight guarantees
- deployment validation
- operator understanding
- run trace legibility
- future studio friendliness

The late-bound path is valuable, but should be treated as an advanced explicit
mode rather than the universal center.

## How this maps onto the current crate shape

Current files:
- `src/types.rs`
  - connection config types
  - generic input/output payloads
  - model-selection types
- `src/runtime.rs`
  - helper functions for bound-vs-late-bound decision flow
- `src/ops/evaluate.rs`
  - reusable semantic operation metadata + skeleton invoke path
- `src/actions/evaluate.rs`
  - thin canonical node wrapper

## Intended API direction

The current crate is moving toward a model where the reusable operation can
accept an explicit selector describing how the model is chosen.

Conceptually:
- no selector => use bound connection
- explicit late-bound selector => use typed runtime-provided refs

That keeps the canonical node simple while still leaving room for more dynamic
usage patterns.

## Resource semantics

### Blob
Blob is the design-center source substrate for:
- workbook baseline bytes
- manifest bytes

### Workspace
Workspace is only for:
- optional emitted evaluated workbook artifacts
- debug/evidence outputs

The source-of-truth workbook should not default to workspace.

## Why this matters for the current build

Without this framing, the scaffold could accidentally drift into one of two bad
states:

### Bad state 1 — over-restricted
Everything must be deployment-bound and static.

Problem:
- dynamic user-supplied models become awkward or impossible

### Bad state 2 — muddy/unbounded
Anything can pass arbitrary objects/paths around and the connector just figures
it out.

Problem:
- weakens preflight and violates the platform ethos

The current target is the middle path:
- strong bound default
- explicit late-bound advanced path
- no ambient unbound access

## Current implementation status

The local scaffold is still intentionally incomplete.

Implemented now as scaffold + first real execution slice:
- type shapes
- connector op metadata
- node wrapper
- basic policy helpers
- connector runtime connection-config resolution seam for bound mode
- resolution-aware preflight hook for bound mode (derived blob requirements)
- real workbook/manifest loading for:
  - workbook blob source
  - workbook file-path source (native only)
  - inline manifest YAML
  - manifest blob source
  - manifest file-path source (native only)
- real Formualizer workbook loading + SheetPort session invocation
- workspace artifact export for the evaluated workbook snapshot
  - gated by connection policy
  - gated by invocation flag
  - written to run-scoped workspace, not back to the source blob
- tests proving:
  - bound connection config resolution
  - invalid bound-config rejection
  - late-bound mode bypassing connector runtime
  - canonical node vs internal-op parity
  - real bound execution
  - real late-bound execution
  - missing blob capability / missing source-object failures
  - workspace export success
  - workspace export policy enforcement
  - missing workspace failure when export is requested
  - representative bound-mode preflight requiring blob before execution
- representative API example under `examples/resolution_pattern.rs`
- representative flow package under `examples/s12_sheetport_quote/`

Not implemented yet:
- richer source kinds beyond the first set
- generalized export support for complex binding shapes (not just the currently handled cell/range-oriented cases)
- full diagnostics polish

## Practical implication for follow-on implementation

The next runtime/API work should preserve this ordering:

1. bound connection mode first
2. explicit late-bound typed refs second
3. only later consider broader generic asset abstractions if more families need
   them

That keeps the current scaffold aligned with Lattice’s ethos while still
avoiding an architectural dead-end.
