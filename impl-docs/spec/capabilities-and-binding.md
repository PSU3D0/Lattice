Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2025-12-12

# Capabilities and Binding (0.1.x)

This document defines how nodes declare capability requirements and how deployments bind concrete providers.

Source of truth:
- Capability interfaces + hint ids: `crates/capabilities/src/lib.rs`
- Hint inference: `crates/capabilities/src/hints.rs`
- Macro hint emission: `crates/dag-macros/src/lib.rs`
- Validation rules: `crates/kernel-plan/src/lib.rs`
- Binding examples:
  - Axum host route config: `crates/host-web-axum/src/lib.rs` (`RouteConfig.resources`)
  - In-proc runtime: `crates/host-inproc/src/lib.rs` (`HostRuntime::with_resource_bag`)

## Capability Domains

In 0.1, capabilities are grouped into coarse domains. Canonical hint ids:
- HTTP: `resource::http`, `resource::http::read`, `resource::http::write`
- KV: `resource::kv`, `resource::kv::read`, `resource::kv::write`
- Blob: `resource::blob`, `resource::blob::read`, `resource::blob::write`
- Queue: `resource::queue`, `resource::queue::publish`, `resource::queue::consume`
- Dedupe: `resource::dedupe`, `resource::dedupe::write`
- DB: `resource::db`, `resource::db::read`, `resource::db::write` (interface may evolve)
- Clock/RNG: `resource::clock`, `resource::rng`

## Declaring Requirements

Nodes declare requirements indirectly via hint emission.

- Macro-authored nodes attach `effectHints[]` and `determinismHints[]` in Flow IR.
- Hints are inferred via `capabilities::hints::infer(...)` using the resource alias and identifier.

Validator contract (implemented):
- If a node declares `effects` that are below the minimum required by its hints, validation fails.
- If a node declares `determinism` stricter than allowed by its hints, validation fails.
- Effectful nodes must declare idempotency metadata.

## Binding Providers (Opaque Binding in 0.1)

Binding is host-specific and occurs at the deployment boundary.

- A deployment constructs a `ResourceBag` (implements `ResourceAccess`).
- Hosts attach this bag to invocations/execution.

Examples:
- Web/Axum host binds resources per route (`RouteConfig.resources`).
- In-proc runtime binds a bag once for the runtime (`HostRuntime::with_resource_bag`).

In 0.1, the concrete infra/provisioning is considered out-of-band.

## Preflight Checks

Hosts SHOULD perform preflight checks before serving traffic:
- Derive required domains from Flow IR `effectHints[]` (not `determinismHints[]`).
- Ensure the configured `ResourceBag` provides compatible providers.
- If required capability bindings are missing, hosts SHOULD fail fast with `CAP101`.

Preflight invariants (0.1.x):
- Unknown `resource::*` effect hints MUST be treated as missing (fail-fast), so new domains cannot silently ship without bindings.
- Hosts SHOULD surface `details.hints[]` as a deterministic list (sorted, unique) for stable conformance and agent tooling.

Preflight is intentionally domain-level in 0.1:
- "This flow needs HTTP write" rather than "this flow needs kv.consistency=Strong".

## Deferred (Explicitly Not 0.1)

To avoid locking in the wrong abstraction too early, these are deferred:
- Capability facets/properties (consistency, durability, residency, etc) as structured requirements.
- `caps.lock` or provider selection solvers.
- Terraform-driven infra generation.

These can land after the 0.1 contract is stable, without changing Flow IR core fields.
