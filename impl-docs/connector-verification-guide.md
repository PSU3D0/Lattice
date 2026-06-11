Status: Active
Purpose: verification-harness contract for connector families
Owner: Connectors
Last reviewed: 2026-06-11

# Connector Verification Guide (0.1.x)

Connector families are hand-written by agents, not codegen'd. The platform's
leverage is therefore **ground truth**: a fixed set of proofs every family must
carry so a reviewer (human or agent) can trust a connector it has never read.
This document defines those proofs, the canonical test layout, and the exact
commands that gate acceptance.

Reference implementations:
- `crates/connectors/google/sheets` — richest runtime surface (semantic ops,
  multi-request actions, handwritten + request-mapped mix).
- `crates/connectors/github/issues` — the canonical *harness-complete* family;
  retrofitted to this guide and the template for new families.

Related specs: `impl-docs/spec/connector-crate-surface.md`,
`impl-docs/spec/capabilities-and-binding.md`,
`impl-docs/spec/example-authoring-conventions.md`, `impl-docs/error-codes.md`
(CAP110 remediation).

## Test layout (fixed)

```
crates/connectors/<vendor>/<family>/
  connector.yaml              # manifest: the public claim surface
  src/...                     # ops (ConnectorOpMetadata + invoke), actions (def_node), runtime
  tests/
    manifest.rs               # proof (a): manifest/metadata honesty
    contract.rs               # proof (b1): registration contract
    runtime.rs                # proof (b2)+(c): canned-transport behavior, errors, pagination, auth
    honesty.rs                # proof (d1)+(e): scoped-bag capability honesty, idempotency evidence
    live_smoke.rs             # proof (f): env-gated live smoke, ignored by default
  examples/local-flow/        # proof (g): end-to-end `flow!` usage, workspace member
```

All tests except `live_smoke.rs` MUST be deterministic and offline: canned
transports via `httpmock::MockServer` plus `connectors_std::dev::EnvConnectorRuntime`
(endpoint/auth resolved from env vars the test sets and restores via an
`EnvGuard`; copy the pattern from `github/issues/tests/runtime.rs`). Never call
a real API from a default test run.

## (a) Manifest / metadata honesty — `tests/manifest.rs`

What it proves: the embedded `connector.yaml` parses and validates under
`connector-spec`, and every action surface agrees with the generated/handwritten
`ops::*::META` (`dag_core::ConnectorOpMetadata`) the kernel actually trusts at
plan, lock, and preflight time. Lies here poison `bindings.lock` generation and
preflight, so they must die in unit tests.

Required assertions (see `github/issues/tests/manifest.rs`):
1. `ConnectorManifest::from_yaml_str(CONNECTOR_YAML)` parses; `.validate()` passes;
   `connector.id` and `crate` match the crate.
2. Action count == op metadata count (no shadow ops, no undeclared surfaces).
3. Per action surface, against the matching `META`:
   - `operation_id` == surface identifier;
   - `min_effects` == the surface's declared effects level;
   - declared `resources(...)` map 1:1 onto `META.effect_hints`
     (`http_read(...)` → `resource::http::read`, etc. — use the
     `capabilities::*::HINT_*` constants, never string literals);
   - an `EndpointProfile` role exists for the surface's endpoint;
   - an `OutboundAuth` role exists iff the surface declares `auth`.

Command: `cargo test -p <crate> --test manifest`

## (b) Contract tests per op — `tests/contract.rs` + `tests/runtime.rs`

Registration contract (`contract.rs`): under the `host-bundle` feature,
`register_all` binds a handler for every operation identifier. One test,
exhaustive over ops.

Runtime contract (`runtime.rs`), per op against canned transport fixtures:
1. **Success path**: the op issues exactly the expected request (method, path,
   headers including endpoint default headers, query, body — assert with
   `when.json_body_obj`/`query_param`) and decodes the canned response into the
   typed output. `mock.assert()` proves the request fired exactly once.
2. **API error mapping**: at least one non-2xx fixture per op family (e.g. 404
   on get, 422 on create). The failure must surface
   `ConnectorRuntimeError::HttpStatus { status, body }` with the provider's own
   error message preserved (truncated to 240 chars by `connectors-std`), and the
   `def_node` action wrapper must keep both visible in the `NodeError` string.
3. **Pagination, if applicable**: multi-page follow via the declared mechanism
   (e.g. `link_header_next` across two mocked pages), AND early termination —
   `limit` truncates without fetching further pages (`assert_eq!(page_two.hits(), 0)`).
4. **Connector-op reuse**: one custom `def_node` with `connector_ops(...)`
   invoking an op, asserting the node spec auto-hoists `effects`, `determinism`,
   `effect_hints`, and `connector_ops` from `META` (drift here breaks the
   declared-equals-enforced promise for downstream flows).

Command: `cargo test -p <crate> --test contract --test runtime`

## (c) Auth-role coverage — in `tests/runtime.rs`

What it proves: every declared `outbound_auth` role is exercised, and
misconfiguration fails actionably *before* any bytes leave the process.

Required, per declared auth role:
1. A success-path test asserting the credential lands where the profile says
   (e.g. `Authorization: Bearer <token>` header asserted by the mock).
2. A misconfiguration test: with the auth env var unset, the op fails with an
   error naming the role AND the env var
   (`MissingAuthOverride { role_name, env_var }` via `EnvConnectorRuntime`),
   and the mock records **zero** hits.
3. Ops without an auth role must succeed with no credential present (proves the
   role list is not under-declared).

Endpoint roles get coverage for free: every offline test overrides the
endpoint-profile base URL env var to point at the mock server.

Command: covered by `cargo test -p <crate> --test runtime`

## (d) Capability honesty + idempotency evidence — `tests/honesty.rs`

**Scoped-bag honesty (CAP110).** Since packet A2, every node executes against a
`capabilities::scoped::ScopedResources` view built from its declared effect
hints; undeclared access fails closed at runtime with a structured CAP110
denial. The honesty tests prove each op's declaration is both *sufficient* and
*load-bearing* — i.e. `effect_hints` neither under- nor over-claim:

1. Per op: execute the op inside
   `ScopedResources::new(op_id, full_bag, META.effect_hints parsed via EffectHint::parse)`
   — a bag granting **only** the declared hints. The op must succeed and
   `take_denials()` must be empty. Run the paginated op with follow-up pages so
   follow-ups are proven in-scope too.
2. Negative: execute at least one read op and one write op under an empty grant
   set. Expect `MissingHttpRead`/`MissingHttpWrite` (or the family equivalent)
   and a recorded denial whose message carries `CAP110`.

`ScopedResources` deliberately passes through `connector_runtime()` /
`connector_scope()` (declared via `connector_ops`, not hints) — do not grant
extra hints to make those work.

**Idempotency evidence (Effectful ops only).** Effectful ops compose with the
dedupe gate that `Delivery::ExactlyOnce` requires at plan time (kernel-plan
`check_exactly_once_requirements`). Prove the composition offline via duplicate
injection:

1. Deliver the same logical request N≥3 times, each attempt gated by
   `DedupeStore::put_if_absent` on a stable idempotency key
   (`connectors_std::dev::MemoryDedupeStore` — in-memory, TTL-correct, no
   containers). Assert exactly one applied, N−1 blocked, and `mock.hits() == 1`.
2. Certify the gate store with the shared harness:
   `testing_harness_idem::verify_dedupe_store(...)` and assert
   `report.passed()`.

If the provider offers a native idempotency key (e.g. an `Idempotency-Key`
header), additionally assert the op sends it; that is provider-side evidence,
not a substitute for the gate test.

Command: `cargo test -p <crate> --test honesty`

## (e) Live-smoke separation — `tests/live_smoke.rs`

Live smoke proves auth + endpoint + decode against the real API. It is double
gated and NEVER part of default CI:
- the test is `#[ignore = "live smoke: ..."]`, so plain `cargo test` skips it;
- it asserts `LATTICE_LIVE_SMOKE=1`, so blanket `--ignored` sweeps cannot hit
  the network by accident.

Keep live smoke **read-only** from tests. Effectful live verification belongs in
a dedicated example binary a human runs deliberately (see
`examples/s11_lead_intake/src/bin/live_smoke.rs` for that pattern).

Command (deliberate, local only):

```sh
LATTICE_LIVE_SMOKE=1 LATTICE_CONNECTOR_AUTH_<ROLE>=... \
  cargo test -p <crate> --test live_smoke -- --ignored --nocapture
```

## (f) Example local-flow crate — `examples/local-flow/`

What it proves: the connector is usable end-to-end from a real `flow!` — the
generated actions register, the IR validates, and the kernel executor runs the
flow under **enforced** scoped resources (the executor builds `ScopedResources`
from each node's hints, so this is the integration-level CAP110 honesty check).

Shape (copy `github/issues/examples/local-flow`; conventions in
`impl-docs/spec/example-authoring-conventions.md`):
- workspace member `example-connector-<vendor>-<family>-local-flow`, registered
  in the root `Cargo.toml` members list;
- `dag_macros::flow!` with a trigger node, at least one connector action node,
  a capture node, and an `entrypoint!`;
- a test asserting the flow IR contains the connector op identifier;
- a test executing the bundle via `bundle().executor().with_resource_bag(...)`
  `.run_once(...)` against a mock server, asserting the typed output;
- README documenting `flows run local --example ... --bindings-lock <lock.json>`
  and how to generate the lock (`flows bindings lock generate`).

Command: `cargo test -p example-connector-<vendor>-<family>-local-flow`

## Acceptance ladder (run all, in order)

```sh
cargo test -p <crate>                                          # (a)-(e), live smoke ignored
cargo test -p example-connector-<vendor>-<family>-local-flow   # (f)
cargo test -p connectors-std                                   # only if you touched shared helpers
cargo check --workspace                                        # no collateral damage
```

A family is harness-complete when all four are green and every section above
has at least the required tests. The dispatchable work-order template for
building a new family against this guide lives at
`ops/packet-template-connector-family.md` (wrapper repo).

## What the retrofit caught (worked example)

Retrofitting `github/issues` (previously: registration contract, success paths,
one pagination test, bearer-auth success) surfaced these gaps — the same list a
new family would have shipped with absent this guide:
1. Manifest was only checked for ID embedding — never parsed, validated, or
   cross-checked against `ops::*::META` (no drift detection between
   `connector.yaml` claims and kernel-visible metadata).
2. No scoped-bag honesty tests: nothing proved `effect_hints` were sufficient
   (ops run under CAP110 enforcement in real flows) or load-bearing.
3. No API error-mapping fixtures: a 404/422 from GitHub had no test pinning the
   status + provider message into the surfaced error.
4. No auth misconfiguration test: a missing PAT's failure mode (actionable
   error naming role + env var, zero outbound requests) was unverified.
5. No pagination early-termination test: `limit` truncation without fetching
   the next page was unverified.
6. No idempotency evidence for the Effectful `create` op.
7. No live-smoke entry point at all (and therefore no enforced separation).
