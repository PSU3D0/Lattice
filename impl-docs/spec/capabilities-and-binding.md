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

## Capability Metadata + Requirements (Planned)

The `resource::*` hint namespace is the portability boundary:
- Flows declare `resource::*` requirements (via hints).
- Deployments bind concrete providers (in-proc, managed service, sidecar proxy, etc).
- Infra provisioning is a separate step (can be generated later from requirements + bindings).

This section defines a minimal, capability-class-specific metadata model plus an MVP requirement language so hosts can reject "present but incompatible" bindings (e.g. Cloudflare KV eventual consistency).

### Provider Metadata

Each capability class SHOULD expose a typed descriptor (e.g. `KeyValue::info() -> KvCapabilityInfo`).

Descriptor shape (conceptual):
- **Standard invariants**: portable, stable fields that every provider must populate (use explicit `unknown` values when needed).
- **Extensions**: `BTreeMap<String, JsonValue>` for provider/plugin-specific metadata (keys must be namespaced, e.g. `com.cloudflare.kv.*`).

Provider variability (config + infra):
- Capability metadata MUST be derived from the concrete provider instance + binding config and reflect guaranteed semantics.
- If a provider cannot guarantee (or cannot safely detect) an invariant, it SHOULD report `unknown` for that field.

Adapter pattern:
- Prefer generic adapters (e.g. Redis/Valkey) with explicit config that determines semantics (read preference, replicas, cluster mode).
- Portable semantics map to standard invariants; product/vendor features belong under `extensions`.

### Requirement Language (MVP)

Requirements are evaluated by hosts during preflight (after a provider is bound).

Operators:
- `any_of`: the provider's level must be one of the listed levels.
- `at_least`: the provider's level must be >= the requested level.

JSON encoding:
- `{"any_of": ["..."]}`
- `{"at_least": "..."}`

Unknown handling (safe default):
- If a requirement is set and the provider reports `unknown`, preflight MUST fail deterministically.
- If the requirement is unset, `unknown` is allowed.

Ordered enums (supporting `at_least`):
- Every ordered invariant uses an intentionally coarse, totally-ordered `*Level` enum.
- Providers MAY expose finer details as separate fields (or under `extensions`) without affecting ordering.

### Capability Inventory (Modeled Today)

Domains (hint ids) and current provider interfaces live in `crates/capabilities/src/lib.rs`.

- HTTP (`resource::http*`): `HttpRead`, `HttpWrite`
- KV (`resource::kv*`): `KeyValue`
- Blob (`resource::blob*`): `BlobStore`
- Queue (`resource::queue*`): `Queue`
- Dedupe (`resource::dedupe*`): `DedupeStore`
- DB (`resource::db*`): hints + constraints only (no provider trait yet)
- Clock/RNG (`resource::clock`, `resource::rng`): `Clock`, `Rng`
- Cache: `Cache` (currently not expressed via `resource::*` hints)

### MVP Requirement Structs (Per Capability Class)

Defaults:
- Every requirement struct is optional; missing/empty means "unconstrained" (only `CAP101` presence checks apply).
- Extension constraints are allowed but are considered vendor-locked; deployments/policies may choose to forbid them.

Common ordered levels (MVP):
- `ConsistencyLevel`: `eventual < strong`
- `DurabilityLevel`: `ephemeral < durable`
- `TtlSupportLevel`: `none < namespace_default < per_write`
- `DeliveryLevel`: `at_most_once < at_least_once < exactly_once`
- `OrderingLevel`: `unordered < ordered`
- `DedupeReservationLevel`: `best_effort < atomic`

MVP requirement structs (conceptual JSON shape):

- `HttpRequirements`
  - `extensions` (object, optional)

- `KvRequirements`
  - `consistency`: `{any_of|at_least}` over `ConsistencyLevel` (optional)
  - `ttl`: `{any_of|at_least}` over `TtlSupportLevel` (optional)
  - `durability`: `{any_of|at_least}` over `DurabilityLevel` (optional)
  - `extensions` (object, optional)

- `BlobRequirements`
  - `consistency`: `{any_of|at_least}` over `ConsistencyLevel` (optional)
  - `durability`: `{any_of|at_least}` over `DurabilityLevel` (optional)
  - `extensions` (object, optional)

- `QueueRequirements`
  - `delivery`: `{any_of|at_least}` over `DeliveryLevel` (optional)
  - `ordering`: `{any_of|at_least}` over `OrderingLevel` (optional)
  - `extensions` (object, optional)

- `DedupeRequirements`
  - `reservation`: `{any_of|at_least}` over `DedupeReservationLevel` (optional)
  - `consistency`: `{any_of|at_least}` over `ConsistencyLevel` (optional)
  - `extensions` (object, optional)

- `ClockRequirements`
  - `extensions` (object, optional)

- `RngRequirements`
  - `extensions` (object, optional)

- `CacheRequirements`
  - `extensions` (object, optional)

- `DbRequirements`
  - `extensions` (object, optional)

## Still Deferred

To avoid locking in the wrong abstraction too early, these remain deferred:
- A canonical provider selection solver (`caps.lock`).
- Terraform-driven infra generation.

Toward local + infra automation:
- Introduce a bindings-plan artifact that maps required `resource::*` domains to provider types + configs (secrets referenced by name only).
- Provisioners can materialize those bindings per environment (docker-compose for local, Terraform modules for cloud) and output a resolved bindings file consumed by hosts.

These can land after the 0.1 contract is stable, without changing Flow IR core fields.
