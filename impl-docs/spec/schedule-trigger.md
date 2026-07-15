Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2026-07-02

# Schedule (Cron) Trigger (v1)

Design for the schedule trigger: authoring surface, IR/manifest shape,
validation, execution semantics, and host mapping. Implemented by packets
T1–T4 of `ops/phase1-clone-engine-plan-2026-06-12.md`; the `[triggers].crons`
rendering contract in §7c is consumed by W1.

Related docs:
- `impl-docs/spec/flow-ir.md` (triggers & entrypoints; 0.1.x compat rules)
- `impl-docs/spec/flow-requirements.md` (manifest shape, versioning policy)
- `impl-docs/spec/connector-trigger-runtime-contract.md` (connector-owned
  polling/webhook triggers — a schedule trigger is NOT one of those; it is a
  host-native ingress like HTTP, with no connector state)
- `impl-docs/error-codes.md` (diagnostic registry)

## 1. Goals / non-goals

Goals: cron-fired flows on Cloudflare Workers and in local dev, with exact
validation parity with what CF accepts and fires; static derivability into
`FlowRequirements` so the wrangler renderer needs nothing but the manifest.

Non-goals (v1): timezones (CF crons are UTC-only; no `timezone` field),
missed-fire recovery/backfill, cross-fire concurrency guards, connector
polling triggers, catch-up semantics.

## 2. Authoring surface

```rust
use dag_core::ScheduledEvent;

#[def_node(trigger, name = "Tick", effects = "Pure", determinism = "Strict")]
async fn tick(event: ScheduledEvent) -> NodeResult<ScheduledEvent> { Ok(event) }

dag_macros::flow! {
    // ...
    let tick = node!(tick);
    let report = node!(report);
    connect!(tick -> report);
    entrypoint!({
        trigger: "tick",
        capture: "report",
        schedule: "*/5 * * * *",
        deadline_ms: 30000,       // optional execution budget, unchanged meaning
    });
}
```

Grammar (extends the existing `EntrypointEntry` parser in dag-macros):

- `schedule: <string literal>` — exactly one 5-field cron expression in the
  Cloudflare dialect, evaluated in UTC. An array form (`schedule: [...]`) is
  reserved for a future extension and rejected in v1.
- `schedule` is **mutually exclusive** with `method` and `route_aliases`
  ([TRIG002] macro error). An entrypoint is either HTTP-shaped or
  schedule-shaped, never both. `deadline_ms` remains allowed (it bounds the
  run, not a response).
- `trigger`/`capture` semantics are unchanged: `capture` names the terminal
  node whose completion ends the run; its output is logged/discarded rather
  than delivered to a caller. DAG204 responder checking applies as today.

**Multiple schedules per flow: allowed.** Each cadence is its own
`entrypoint!` statement with a **distinct trigger alias** (the macro already
derives an entrypoint const per trigger alias, so this falls out of the
existing uniqueness rule). Two cadences driving the same DAG = two trigger
nodes fanning into the shared downstream. A flow mixing HTTP and schedule
entrypoints is allowed but requires the existing
`policies.allow_multiple_triggers` opt-in (DAG104), and one trigger alias may
not be wired to both kinds ([TRIG003]).

**`ScheduledEvent` lives in dag-core** (`dag_core::trigger::ScheduledEvent`,
re-exported at the root):

```rust
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ScheduledEvent {
    /// Scheduled fire time, epoch milliseconds, UTC.
    pub scheduled_time_ms: u64,
    /// The cron expression that fired, byte-identical to the authored string.
    pub cron: String,
}
```

Rationale for the home: the type is referenced by macro-generated typed
entrypoints, kernel-plan, both hosts, and examples; dag-core is the shared,
wasm32-clean vocabulary crate already on every one of those dependency paths.
stdlib holds node *implementations*, and hosts must not depend on it.

The macro emits a compile-time assertion that a schedule entrypoint's trigger
input type is **exactly** `dag_core::ScheduledEvent` (rustc type error +
trybuild case; no `Into`-flexibility in v1).

## 3. IR and registry shape (additive only)

| Type | New field | Serde behavior |
| --- | --- | --- |
| `dag_core::EntrypointMetadata` (Flow IR) | `schedule: Option<String>` | `#[serde(default, skip_serializing_if = "Option::is_none")]` |
| `dag_core::flow_registry::EntrypointSpec` | `schedule: Option<&'static str>` | not serialized (const; all constructions are macro-generated and updated together) |
| `host_inproc::FlowEntrypoint` | `schedule: Option<String>` | not serialized |

Existing flows emit byte-identical IR: the only serialized addition is
skip-when-absent. `schemas/flow_ir.schema.json` is regenerated via
`cargo run -p dag-core --bin emit_schemas` (the `schemas_match_rust_emission`
test enforces this). Per flow-ir.md 0.1.x rules this is a legal additive
optional field.

## 4. Validation and dependency decision

Cron expressions are parsed at **macro expansion time** (span-carrying error,
best UX) and again at **kernel-plan time** (covers hand-built IR). Invalid
cron fails closed: the flow will not plan, bundle, or deploy. UTC-only is
part of the v1 contract — no timezone syntax is accepted.

New diagnostic family `TRIG0xx` (no `TRIG` codes exist today; register in
`impl-docs/error-codes.md` and `dag-core/src/diagnostics.rs`, kept in sync by
`diagnostics_registry_matches_doc`):

| Code | Subsystem | Default | Summary |
| --- | --- | --- | --- |
| TRIG001 | Validation | Error | Schedule expression is not a valid Cloudflare-dialect cron. Includes expressions that parse but can never fire (e.g. `0 0 31 2 *`) — CF's validate endpoint rejects those too (saffron `Cron::any()`). |
| TRIG002 | Macros | Error | `schedule` conflicts with `method`/`route_aliases` on one entrypoint. Enforced at macro expansion (span-carrying) AND by kernel-plan on the IR, so hand-built IR cannot smuggle a both-shaped entrypoint past the TRIG003 disjointness argument. |
| TRIG003 | Validation | Error | Trigger alias wired to both schedule and HTTP entrypoints. |
| TRIG004 | Validation | Error | Duplicate schedule entrypoint (same cron + trigger alias). |

T1 implementation note: kernel-plan's TRIG001 saffron parse is compiled only
for `cfg(not(target_arch = "wasm32"))` (saffron sits in kernel-plan's
target-gated dependencies). On wasm32 the cron-syntax check is a no-op — IR
always passes host-side validation at plan/bundle/deploy time before it can
reach a wasm host, and host-workers routes fires by byte equality (§7a).
TRIG002/003/004 run on every target.

**Parser dependency: `saffron` (recommended).** Verified 2026-07-02:

| Crate | License | Parity with CF | wasm32 | Maintenance |
| --- | --- | --- | --- | --- |
| `saffron` 0.1.0 | BSD-3-Clause (repo LICENSE; crates.io shows "non-standard" because Cargo.toml uses `license-file`) | **Exact — it IS the parser Cloudflare runs for Cron Triggers** (backend, dashboard, edge validate endpoint) | yes (`no_std`+alloc; chrono default-features off, nom) | repo active (pushed 2026-04); crates.io release frozen at 0.1.0 since 2021 |
| `croner` 3.0.1 | MIT | JS-croner dialect; DOM/DOW combination semantics differ from CF | yes | active |
| `cron` 0.17.0 | MIT OR Apache-2.0 | Quartz-style 6/7-field (seconds), different DOW numbering — rejects/accepts differently from CF | yes | active |

The parity argument is decisive: local validation must never accept an
expression CF rejects, and the dev scheduler must fire at the times CF would.
Only saffron guarantees both by construction. BSD-3-Clause is compatible.
The frozen crates.io release is acceptable because CF production pins the
same code; if upstream fixes land unreleased, pin the git rev (record the pin
rationale in the workspace `Cargo.toml` comment). saffron is a dependency of
dag-macros, kernel-plan, and the dev scheduler only — it is **kept out of the
shipped wasm**: host-workers routes by string equality (§7a) and never parses.

## 5. Execution semantics

- **At-least-once.** A single cron fire may be observed more than once
  (platform retries, our own dev-loop `--once` replays). Exactly-once is not
  offered.
- **Typed payload.** The trigger node receives
  `ScheduledEvent { scheduled_time_ms, cron }`. `scheduled_time_ms` is the
  *scheduled* time, not the observed wall clock, so it is stable across
  redeliveries of the same fire.
- **Idempotency guidance.** Effectful downstream nodes should key on the
  scheduled time, e.g. `key = "<flow>:<trigger>:{scheduled_time_ms}"`:
  redeliveries of one fire dedupe, distinct fires stay distinct. The
  existing IDEM/EXACT validation machinery applies unchanged.
- **Missed fires.** CF cron fires are best-effort; under load they may be
  delayed or skipped. v1 documents this and does nothing: no catch-up, no
  backfill. Flows that must detect gaps should keep a last-processed
  watermark (KV) and compare against `scheduled_time_ms` (guidance only).
- **Overlap: allowed in v1.** If a run outlives its interval, the next fire
  starts concurrently. Justification: CF invokes `scheduled()` concurrently
  anyway; preventing overlap requires a distributed lease (DO-backed),
  which is durability machinery out of scope for v1; idempotency-on-
  scheduled-time makes the hazard bounded. An `overlap: skip | queue`
  entrypoint field is reserved for v2.

## 6. FlowRequirements

Additive fields (all skip-when-absent — existing manifests stay
byte-identical):

- `TriggerKind` gains unit variant `Schedule` (serialized `"schedule"`). It
  stays a plain `Copy` unit enum; crons are NOT embedded in the variant
  because that would change the `kind` field's JSON shape (string → object).
- `TriggerRequirement` gains `crons: Vec<String>`
  (`skip_serializing_if = "Vec::is_empty"`): the schedules of entrypoints
  wired to that trigger alias (exactly one in v1).
- `EntrypointRequirement` gains `schedule: Option<String>`.

Derivation rule update (flow-requirements.md table): trigger `kind` is
`schedule` when the alias is wired to an entrypoint carrying `schedule`,
`http` when wired to one without, `unspecified` otherwise. TRIG003
guarantees the cases are disjoint.

**At T1, `schema_version` stayed `0.1` (P4a later bumps the manifest to `0.2`).** Policy reads "additive optional fields do
not bump it"; the one judgment call is the new `TriggerKind` value, which
this packet resolves as a *tolerated additive value*: flow-requirements.md
must document that consumers encountering an unknown trigger `kind` treat
that flow as "cannot place" (fail closed per-flow) rather than reject the
manifest. This preserves bundle-id stability for every existing flow (a
version bump would rewrite all manifests and hashes). Maintainer may veto to
`0.2`; nothing else in this design changes if so.

**Goldens:** the three existing fixtures are untouched. T1 adds a schedule
fixture (`crates/kernel-plan/tests/fixtures/*_schedule.requirements.json`)
plus its golden test, and regenerates `flow_requirements.schema.json`.

## 7. Host mapping

### 7a. host-workers `scheduled()` handler (T3)

New `#[event(scheduled)]` handler (behind the existing `entrypoint` feature)
alongside `fetch`. `worker::ScheduledEvent` supplies `cron()` (the trigger's
original expression string) and `schedule_time()` (epoch ms).

**Routing rule:** candidate set = every entrypoint in the loaded bundle with
`entry.schedule` **byte-equal** to `event.cron()`. No parsing, no
normalization, ever — parity holds because W1 renders cron strings verbatim
from the manifest (§7c) and CF echoes the configured string back in the
event. For each candidate, build
`Invocation::new(trigger_alias, capture_alias, json!(ScheduledEvent{..}))`
`.with_deadline(entry.deadline)` and execute via the same
`runtime_from_bundle` path as fetch; the capture output is logged and
discarded (no Response).

- **Collision case:** two entrypoints (same or different flows once
  `get_bundle()` grows multi-flow) declaring the same cron string is a
  defined **fan-out**, not an error: CF fires that cron once per worker, and
  the handler invokes every candidate sequentially in deterministic order
  (flow name, then entrypoint declaration order). The renderer dedupes the
  crons list accordingly (§7c).
- **Zero matches** = configuration drift between wrangler.toml and the
  bundle (e.g. dashboard-edited crons). Fail loudly: log
  `unroutable cron fire: <cron>` and return an error so it surfaces in tail
  logs. Never fire "the only flow" as a fallback.
- Execution failures propagate out of `scheduled()` (visible to observability;
  CF does not retry cron failures — at-least-once comes from the platform's
  own redelivery behavior, not from us).

### 7b. host-inproc dev scheduler (T2)

`flows run schedule --example <name> [--once | --at <rfc3339>] [--trigger <alias>]`

- Pure core in host-inproc:
  `next_fires(after: DateTime<Utc>, entrypoints) -> Vec<(DateTime<Utc>, &FlowEntrypoint)>`
  computed with saffron (same parser as validation — dev fires match CF
  fires). The tick loop in the CLI is
  `loop { sleep_until(next); execute synthetic ScheduledEvent; }` with the
  clock and sleep injected (trait or fn pointer) so tests step a mocked clock
  through fires without real sleeps.
- `--once` fires every schedule entrypoint (or the `--trigger` subset)
  immediately with `scheduled_time_ms = now` (or `--at`), prints per-fire
  results, exits non-zero on any failure. This is the agent/test path and
  what T4's Tier C verification drives.
- All computation in UTC.

### 7c. Wrangler rendering contract (consumed by W1)

The renderer reads **only** `FlowRequirements`, never IR:

```
crons(bundle) = sorted, deduplicated union over flow entries of
                entrypoints[].schedule (non-null values), byte-verbatim
```

Emitted as `[triggers]` / `crons = [...]` only when non-empty. Requirements
on the renderer: (1) never normalize/rewrite a cron string — §7a routing
depends on byte equality; (2) dedupe (CF rejects duplicate crons; duplicates
across flows are the §7a fan-out case); (3) warn when the union exceeds the
CF cron trigger cap (5 free / 250 paid, **per account** — verified 2026-07-11
against developers.cloudflare.com/workers/platform/limits; encoded in the
renderer as FREE_MAX_CRONS/PAID_MAX_CRONS, warn-not-abort since the cap is
account-wide and the renderer only sees one worker).

## 8. Packet decomposition (T1–T4 confirmation)

- **T1** (as planned, one scope addition): dag-core (`ScheduledEvent`,
  `TriggerKind::Schedule`, `TriggerRequirement.crons`,
  `EntrypointRequirement.schedule`, `EntrypointMetadata.schedule`, TRIG001–004
  in diagnostics.rs + error-codes.md), dag-macros (grammar, TRIG002 macro
  error, exact-type assertion, saffron parse), kernel-plan (TRIG001/003/004
  on IR), schema regen ×2, schedule golden fixture, trybuild cases (bad cron,
  schedule+method, wrong trigger input type; bump ui-full count).
  **Scope addition:** `host_inproc::FlowEntrypoint.schedule` — the macro
  constructs that struct, so the additive field must land in the same change.
  T1 therefore needs a sliver of LOCK-HOSTS (one struct literal + struct
  def); coordinate with the wave or land it as the first T2 commit.
- **T2**: as planned; put `next_fires` + synthetic-event helper in
  host-inproc, loop/flags in cli, per §7b. Depends on T1.
- **T3**: as planned, per §7a; owns the `[triggers]` arm in W1's renderer if
  W1 lands first (W1 stubs it). workerd test follows the existing
  workerd-tests harness; note miniflare/workerd can dispatch a scheduled
  event via the `/cdn-cgi/handler/scheduled?cron=...` test endpoint — the
  test asserts a cron-fired flow executes with durability + scoped
  capabilities intact and that an unroutable cron errors.
- **T4**: unchanged (s15 example, 5-min GitHub poll → sheet/workspace write,
  Tier C + F1 honesty tests).

## 9. Open questions (maintainer)

1. `schema_version` hold-at-0.1 with tolerated `"schedule"` kind (§6) — veto
   to `0.2` acceptable, decide before T1.
   RESOLVED (coordinator, T1 2026-07-02): hold at `0.1`; `"schedule"` is a
   tolerated additive kind, documented in flow-requirements.md.
2. saffron via crates.io 0.1.0 vs git pin (§4) — default: crates.io.
   RESOLVED by T1 (2026-07-02): crates.io 0.1.0. Verified its API covers
   everything T1/T2 need (`Cron: FromStr` for parse-validation, `Cron::any()`
   for never-fires detection, `Cron::next_after()` for the dev scheduler);
   the pin rationale comment lives next to `saffron` in the workspace
   `Cargo.toml` `[workspace.dependencies]`.
3. Reserved v2 surface (`schedule: [...]` array, `overlap:`, `timezone:`)
   — names reserved here so T1 rejects them with forward-pointing errors.
   Implemented: the entrypoint! parser rejects all three with reserved-name
   errors.

## 10. T1 scope note (bundle-manifest carriage, for T3)

T1 landed `schedule` in Flow IR metadata, `flow_registry::EntrypointSpec`,
`host_inproc::FlowEntrypoint`, and `FlowRequirements`. NOT yet threaded:
`flow_bundle` manifest `Entrypoint` / `exporters::entrypoint_from_spec` /
the bundle-loading paths in host-wasmtime and host-workers construct
`FlowEntrypoint { schedule: None }` from bundle manifests, because the bundle
manifest `Entrypoint` shape does not carry a schedule field yet. T3 must add
it (additive, skip-when-absent — same bundle-id-stability argument as §6)
before `scheduled()` can route on `entry.schedule` from a loaded bundle.
