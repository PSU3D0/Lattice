Purpose: spec
Owner: Core
Last reviewed: 2025-12-15

# Resource Catalog + Bindings (0.1.x)

This document defines the deployment-side files used to:
- declare available resource instances (existing or managed),
- map flows (or groups of flows) onto those instances,
- emit a resolved bindings file that hosts can consume to construct a `ResourceBag`.

This is intentionally **out-of-band** from Flow IR:
- Flows declare capability needs via `resource::*` hints.
- Deployments decide provider selection, credentials, isolation, and infra provisioning.

Related specs:
- Capability domains + hints: `impl-docs/spec/capabilities-and-binding.md`
- Invocation boundary: `impl-docs/spec/invocation-abi.md`

Non-goals (0.1.x):
- A full provider selection solver.
- Terraform module generation as a contract.
- Embedding secrets into IR/artifacts.

## Files

### `resources.catalog.json` (intent)

Human-authored, checked into VCS.

Responsibilities:
- Define named **instances** (e.g. `redis_shared`, `kv_local`).
- Describe each instance's provider kind and supported `resource::*` domains.
- Declare required isolation wrappers (namespacing, key-hmac, encryption) and secret references by name.
- Map flows to instances.

`resources.catalog.json` can be authored manually, or generated from infra state.
For example, Terraform can produce catalog entries by exporting module outputs (endpoints, ids) and secret references
(names/paths), without embedding secret material.

### `bindings.lock.json` (resolved)

Environment-specific, **machine-generated**.

Responsibilities:
- Resolve instance wiring into a concrete bindings map for a set of flows.
- Contain only non-secret material plus secret references by name.
- Provide a host-consumable description for constructing a `ResourceBag`.

Invariants:
- The lock file should be treated as generated output; manual edits are discouraged.
- Resolvers SHOULD embed a deterministic `content_hash` so tools can detect accidental edits or stale state.

## Provider Kinds and Plugin Validation

A *provider kind* is a stable string identifier describing an implementation family.
Examples:
- `redis.generic`
- `redis.upstash`
- `cloudflare.kv`
- `cloudflare.do`

Provider kinds can be "plain" (just connectivity) or "composite" (bake in isolation semantics).
For example:
- `kv.redis` (plain)
- `kv.zero_trust_upstash` (composite: requires scoping/encryption config; enforces it at runtime)

Provider kinds are validated via a **provider registry** (built into the host/CLI binary in 0.1).
Each provider registry entry MUST declare:
- `kind` string
- `provides[]`: the set of allowed `resource::*` domains/hints it can satisfy
- `connect_schema`: JSON schema for `connect` config
- `config_schema`: JSON schema for provider-specific config

Unknown provider kinds MUST fail validation by default.

This is the plugin boundary: provider crates can register new kinds at build time, provided they implement the capability traits and declare their schemas/metadata.

## Isolation Wrappers

Isolation is modeled as an ordered list of **wrappers** applied by the binder.
Wrappers are validated via a wrapper registry.

Wrappers are domain-scoped (not provider-scoped). Examples:
- `isolation.prefix_keys` (KV/Dedupe/Blob)
- `isolation.hmac_keys` (Dedupe)
- `isolation.encrypt_values` (KV/Blob)
- `isolation.sql_schema_prefix` (DB)

Wrappers MUST declare:
- `kind` string
- `applies_to[]` domains
- `config_schema`

### Modeling choices

Deployments may model isolation in two ways:

- Wrapper composition: a "plain" provider kind plus one or more wrappers.
  - Example: `kv.redis` + `isolation.prefix_keys` + `isolation.encrypt_values`.
- Composite provider kinds: provider kind semantics include isolation/zero-trust behavior.
  - Example: `kv.zero_trust_upstash` requires a namespace strategy and key references and enforces them.

Both approaches are valid. Wrapper composition maximizes reuse across backends; composite providers can enforce stronger invariants by construction.

### Shared instances across many flows

A single physical service (e.g. one Postgres cluster or one serverless KV account) can be shared by many flows.
`bindings.lock.json` supports this by letting many `flows.<flow_id>.use[...]` entries reference the same `instances.<name>`.

If a deployment needs per-flow scoping for a shared physical service (e.g. per-flow namespace prefixes), a resolver MAY synthesize derived instances in the lock
(e.g. `kv_shared` plus `kv_flow_<flow_id>` instances) so each flow binds to its own scoped logical instance without changing the lock schema.
A future lock schema MAY add per-use isolation config, but derived instances keep 0.1 compatible.

## Resolver Boundary (MVP)

A resolver consumes intent and produces a concrete lock file.

- Inputs: Flow requirements (from Flow IR `effectHints[]`), a `resources.catalog.json`, and optional policy/environment selectors.
- Output: a `bindings.lock.json` scoped to one or more flows.
- Determinism: the resolver MUST be deterministic given the same inputs (no wall clock dependence; `generated_at` is injected).
- Safety: the resolver MUST NOT fetch secrets or embed secret material; it only carries secret references.

Terraform/pulumi/etc integration belongs on the catalog-production side: those tools can export `resources.catalog.json` from existing infra state and outputs.

## Schema (MVP)

This section describes the canonical JSON shape. YAML is acceptable if it round-trips to the same JSON.

### `resources.catalog.json`

Top-level:
- `version` (number, required)
- `instances` (object map, required)
- `bindings` (array, required)

#### Instance
Each `instances.<name>`:
- `provider_kind` (string, required)
- `mode` (`external|managed`, required)
- `provides` (array of `resource::*` strings, required)
- `connect` (object, required; provider-specific; secret refs by name)
- `config` (object, optional; provider-specific)
- `semantics` (object, optional)
  - Portable invariants MAY be declared here as expectations (e.g. `consistency=strong`).
  - Provider-specific features belong under `extensions`.
- `isolation` (array of wrapper declarations, optional)
- `extensions` (object, optional; provider/plugin-specific)

Notes:
- `semantics` is not a substitute for provider metadata; it is an *assertion/expectation* used to validate bindings.
- If semantics cannot be guaranteed/detected, providers should report `unknown` and flows requiring that invariant should fail preflight.

#### Binding entry
Each element of `bindings[]`:
- `selector` (object)
  - `flow` (string, optional; flow name)
  - `flow_id` (string, optional; uuid)
  - `profile` (string, optional)
  - `tags_any` (array of strings, optional)
- `use` (object map)
  - keys: `resource::*` domain/hint
  - values: instance name from `instances`

Resolution precedence is implementation-defined; recommended: first match wins.

### `bindings.lock.json`

Top-level:
- `version` (number, required)
- `generated_at` (RFC3339 string, required)
- `content_hash` (string, required)
  - `sha256` of canonical JSON of the lock file **excluding** the `content_hash` field.
  - Canonicalization rule: recursively sort object keys; preserve array order.
- `instances` (object map, required)
- `flows` (object map, required)

#### Resolved instance
Each `instances.<name>`:
- `provider_kind` (string)
- `provides` (array)
- `connect` (object; secret refs by name)
- `config` (object; provider-specific)
- `isolation` (array of wrapper declarations)

#### Flow bindings
Each `flows.<flow_id>`:
- `use` (object map)
  - keys: `resource::*` domain/hint
  - values: instance name

## Validation Rules (MVP)

Catalog validation:
- All `provides[]` entries MUST begin with `resource::`.
- For each instance, `provides[]` MUST be a subset of what its `provider_kind` registry entry supports.
- Wrapper kinds in `isolation[]` MUST be known and must apply to at least one of the instance's domains.
- `connect` and `config` MUST validate against the provider kind's declared schemas.
- Secrets MUST be referenced by name only (no secret material in files).

Lock validation:
- Every referenced instance must exist and be well-formed.
- `flows.<flow_id>.use` must only reference `resource::*` keys.

Host preflight (runtime):
- Missing domains still surface as `CAP101`.
- "Present but incompatible" bindings should surface a distinct code (planned) once capability metadata + requirements are implemented.
