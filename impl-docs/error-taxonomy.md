Status: Canonical
Purpose: spec
Owner: Core
Last reviewed: 2025-12-12

# Error & Policy Violation Taxonomy

This note groups the diagnostic codes and policy checks we plan to surface across the platform. Use it as the bridge between the RFC (`impl-docs/rust-workflow-tdd-rfc.md`), the roadmap (`impl-docs/roadmap/epics.md`), and the error-code registry (`impl-docs/error-codes.md`). Each section summarises why an error exists, where it is raised (macro, validator, runtime, certification), and the kinds of Rust code patterns that can trigger it.

## 1. Graph & Topology Safety
- **Duplicates & cycles** — `DAG200`, `DAG205` (RFC §5.4 items 1 & 4). Validator ensures acyclic DAG and unique aliases. Macro catches most duplicates, but IR imported via CLI/importer must be rechecked.
- **Unbound references** — `DAG201`, `DAG202` (RFC §4.3, §5.4 item 2). Edges/control surfaces referencing aliases that don't exist. Importer-generated IR or hand-edited flows can trigger this by misspelling aliases or omitting bindings.
- **Edge control misuse** — `DAG206`, `DAG207`. Macro edge-control statements (`timeout!`, `delivery!`, `buffer!`, `spill!`) referencing a missing `connect!` edge (`DAG206`) or repeating a control for the same edge (`DAG207`).

### Example: Unbound alias (`DAG202`)

```rust
workflow! {
    name: invoice_flow,
    version: "1.0.0",
    profile: Queue;
    let ingest = ingest_orders_node_spec();
    let calculate = calculate_invoice_node_spec();
    connect!(ingest -> calculate);
    connect!(calculate -> missing_alias);   // <- typo triggers DAG202
}
```

**Mitigation:** ensure the downstream alias exists or add the missing node:

```rust
workflow! {
    name: invoice_flow,
    version: "1.0.0",
    profile: Queue;
    let ingest = ingest_orders_node_spec();
    let calculate = calculate_invoice_node_spec();
    let missing_alias = persist_invoice_node_spec();
    connect!(ingest -> calculate);
    connect!(calculate -> missing_alias);
}
```

The same error appears if a `vars!` binding, exporter, or control surface references an undeclared identifier. Validator rule `check_unbound_variables` should inspect all surfaces, not just edges.

## 2. Schema & Type Compatibility
- **Port mismatches** — `DAG201` (RFC §4.1, §5.4). Happens when output schema of upstream node doesn’t match downstream input. Rust-level symptom: forgetting to wrap output in adapter (e.g., `Map<T,U>`), or returning a different type than declared in `#[node]`.
- **View/export serialisability** — `DAG301` (RFC §4.6). Raised if `hitl!`/`view` payloads aren’t JSON-serialisable; code pattern: returning custom types without serde/Schemars derives.

### Example: Port mismatch (`DAG201`)

```rust
#[node(name = "FetchOrders", out = "Vec<Order>")]
async fn fetch_orders(_: ()) -> NodeResult<Vec<Order>> { ... }

#[node(name = "SendEmail", in = "EmailPayload")]
async fn send_email(payload: EmailPayload) -> NodeResult<()> { ... }

workflow! {
    name: wrong_types,
    version: "1.0.0",
    profile: Queue;
    let fetch = fetch_orders_node_spec();
    let mail = send_email_node_spec();
    connect!(fetch -> mail);   // Vec<Order> -> EmailPayload produces DAG201
}
```

**Mitigation:** insert an adapter node converting `Vec<Order>` into `EmailPayload`, or change the receiving node schema. In Rust terms, implement `From<Vec<Order>> for EmailPayload` and use a `Map` helper.

## 3. Capability & Credential Binding
- **Missing credentials** — `DAG203`, `SECR201` (RFC §4.6, §15.2). Validator checks that capabilities needing creds have matching `creds!` bindings, scopes ⊆ requested. Rust misuse: referencing `ctx.resources.http()` without declaring `creds!(...)` in the workflow.
- **Effects mismatch** — `EFFECT201` (RFC §4.1, Appendix C). Node claims `Pure`/`ReadOnly` but requests write capability (e.g., `HttpWrite`). Triggered by inline code calling `ctx.resources.http_write()` without downgrading effects.

### Example: Missing credentials (`DAG203`, `SECR201`)

```rust
#[node(name = "PostWebhook", effects = "Effectful")]
async fn post_webhook(ctx: &mut Ctx<Params, Resources>) -> NodeResult<()> {
    ctx.resources.http().post("https://api.example.com", &payload).await?;
    Ok(())
}

workflow! {
    name: webhook_flow,
    version: "1.0.0",
    profile: Queue;
    let poster = post_webhook_node_spec();
    // connect!(poster -> responder);
    // creds! block missing -> validator raises DAG203 / SECR201
}
```

**Mitigation:** declare credentials with the correct provider + scopes:

```rust
workflow! {
    // ...
    creds! {
        webhook_creds: AwsSecretsManager {
            scope: ["webhook/post"],
        }
    };
    let poster = post_webhook_node_spec();
    // connect!(poster -> responder);
    // ...
}
```

Ensure the node references the credential handle in its params/resources.

- **Registered effect hints** — `EFFECT201`. Resource hints such as `resource::http::write` map to minimum effects levels via `dag_core::effects_registry`. Validators enforce that declared effects are at least as permissive as the hint (e.g., a connector emitting `resource::db::write` must declare `effects = "Effectful"`). Third-party capability crates register additional hints during their init hooks, so new providers inherit the same guardrails without touching core code.

### Example: Effects mismatch (`EFFECT201`)

```rust
#[node(
    name = "ComputeChecksum",
    effects = "Pure",          // Declared Pure
    determinism = "Strict"
)]
async fn compute_checksum(ctx: &mut Ctx<Params, Resources>, input: Bytes) -> NodeResult<Bytes> {
    ctx.resources.http().post("https://logs.example.com", &input).await?; // Write capability
    Ok(input)
}
```

**Mitigation:** either:
1. Change `effects = "Effectful"` to match behaviour, or
2. Remove the HTTP write and keep the node pure.

Validator should compare node effect declaration to capability usage derived by macros or static analysis.

## 4. Determinism Enforcement
- **Resource conflicts** — `DET301` (RFC §8). Nodes declaring `Strict`/`Stable` but using non-deterministic resources: e.g., `Clock`, `Rng`, unpinned URLs. Rust symptoms:
  - Importing `rand::random()` in inline node.
  - Calling `SystemTime::now()` directly.
  - Fetching HTTP without `PinnedUrl`/`HashRef`.
  - Using global mutable state.
  Diagnostics surface at macro expansion (automatic downgrade) or validator (after importer-generated IR).
- **Registered resource hints** — `DET302`. Nodes may carry determinism hints (e.g., `resource::clock`, `resource::rng`) emitted by macros or plugins. The validator consults the shared registry (`dag_core::determinism`) to ensure declared determinism is at least as permissive as the hint requires. Authors can also extend the registry at runtime when shipping new capability crates.
- **Runtime certification** — `DET3xx` family (future). Certification harness replays nodes and compares output; divergence triggers publish failure even if compile passed.

### Example: Non-deterministic inline code (`DET301`)

```rust
use rand::Rng;  // Direct rand import

#[node(
    name = "GeneratePromoCode",
    determinism = "Strict",    // Author claims strict determinism
    effects = "Pure"
)]
async fn generate(_: ()) -> NodeResult<String> {
    let mut rng = rand::thread_rng();
    Ok(format!("PROMO-{}", rng.gen::<u64>()))   // Randomness breaks strict determinism
}
```

**Mitigation:**\n
1. Switch to a deterministic RNG seeded from workflow input (or attach `ctx.resources.rng_seeded()` if policy allows) and keep determinism strict.\n
2. Downgrade determinism to `BestEffort` and let validator emit a warning instead of error if policy permits.

Similarly, calling `SystemTime::now()` or `Instant::now()` in a `Strict` node would trigger `DET301`. Provide deterministic timestamps via input payloads instead.

## 5. Idempotency & Delivery Semantics
- **Missing keys** — `DAG004`, `IDEM020`/`IDEM025` (RFC §5.2 & §5.4). Effectful nodes must define deterministic idempotency keys. Rust-logic mistakes: forgetting to attach `idempotency =` attribute, or adding non-deterministic fields to key expression.
- **Exactly-once prerequisites** — `EXACT001`/`EXACT002`/`EXACT003` and `EXACT005` (RFC §5.4). Validator ensures a dedupe capability binding exists, nodes expose idempotency metadata (key + TTL), and sink contracts are documented. Queue/Temporal hosts also enforce these checks before runtime.

### Example: Missing idempotency (`DAG004`)

```rust
#[node(
    name = "ChargeCustomer",
    effects = "Effectful",
    determinism = "BestEffort"
    // idempotency attribute missing
)]
async fn charge(ctx: &mut Ctx<Params, Resources>, input: ChargeRequest) -> NodeResult<()> {
    ctx.resources.stripe().charge(&input).await?;
    Ok(())
}
```

**Mitigation:** declare idempotency key using deterministic fields:

```rust
#[node(
    name = "ChargeCustomer",
    effects = "Effectful",
    determinism = "BestEffort",
    idempotency = "partition_by!(|req: &ChargeRequest| req.order_id.clone())"
)]
```

If the key references non-deterministic data (e.g., `Uuid::new_v4()`), `IDEM025` should fire.

### Example: Exactly-once without dedupe (`EXACT001`)

```rust
workflow! {
    name: warehouse,
    version: "1.0.0",
    profile: Queue;
    let upsert = upsert_row_node_spec(); // declares `delivery = ExactlyOnce`
    // connect!(upsert -> sink);
    // missing DedupeStore capability -> EXACT001
}
```

Mitigation: bind `cap-dedupe-redis` (or equivalent) via `capabilities` and ensure `UpsertRow` references it.

### Example: Exactly-once without TTL (`EXACT003`)

```rust
#[node(
    name = "UpsertRow",
    effects = "Effectful",
    determinism = "BestEffort",
    idempotency = "partition_by!(|req: &UpsertRequest| req.row_id.clone())"
    // ttl_ms omitted -> EXACT003 (minimum 300_000ms by default)
)]
```

Mitigation: declare an explicit TTL that meets or exceeds the platform minimum (e.g. `ttl_ms = 300_000` for a five minute lease) so the queue bridge can safely dedupe retries.

## 6. Spill Configuration
- **Missing buffer bound** — `SPILL001` (RFC §6.4). Any edge that enables `spill_tier` must still declare an in-memory `max_items` budget so the executor knows when to begin spilling. Rust mistakes: setting `spill_tier = "local"` without bounding the queue, or forgetting to propagate the configuration when composing flows.
- **Missing blob binding** — `SPILL002` (RFC §6.4). Spilling to blob storage requires the flow to bind a blob capability (e.g., `cap-blob-fs`, `cap-blob-s3`) and emit the corresponding `resource::blob::write` hint. Without the hint the validator assumes the runtime cannot persist overflow safely.

### Example: Spill tier without buffer (`SPILL001`)

```rust
builder.connect(&trigger, &worker);
let mut flow = builder.build();
for edge in &mut flow.edges {
    edge.buffer.spill_tier = Some("local".into());
    edge.buffer.max_items = None; // -> SPILL001
}
```

**Mitigation:** specify a finite in-memory buffer (e.g., `max_items = Some(128)`) before enabling spill so the executor can decide when to offload messages.

### Example: Spill without blob capability (`SPILL002`)

```rust
// Worker node lacks resource::blob::write hint
let worker = builder.add_node("worker", &worker_spec_without_blob).unwrap();
// Edge requests spill tier
edge.buffer.spill_tier = Some("s3".into());
edge.buffer.max_items = Some(64);
```

**Mitigation:** bind a blob capability (e.g., `cap-blob-fs`, `cap-blob-s3`) and ensure the emitting node declares `resource::blob::write` so spill operations have a backing store.

## 7. Policy & Compliance
- **Egress restrictions** — `EGRESS101` (RFC §15.1). Workflow requests a provider/egress domain outside policy allowlist.
- **Data classification** — `DATA101` (RFC §5.4, §15.5). Nodes/ports missing data-class tags required for policy evaluation.
- **Cache policy** — `CACHE001`, `CACHE002` (RFC §5.2, §8). Strict nodes without caches or stable nodes without pinned inputs.
- **Retry ownership** — `RETRY010`, `RETRY011` (RFC §7.2.1). Ensures retries are owned by either connector or orchestrator, not both, and align with capability defaults.
- **Timeouts & budget** — `TIME015`, `RUN030`, `RUN040`, `RUN050` (RFC §6.5, §14). Raised by runtime when nodes exceed timeout budgets, experience sustained backpressure, spill, or hit memory limits. Rust patterns: heavy synchronous work blocking executor, forgetting to respect `ctx.is_cancelled()`.

### Example: Cache policy (`CACHE002`)

```rust
#[node(
    name = "FetchExchangeRates",
    effects = "ReadOnly",
    determinism = "Stable"
)]
async fn fetch(ctx: &mut Ctx<Params, Resources>, _: ()) -> NodeResult<Rates> {
    ctx.resources.http().get("https://rates.example.com/latest").await?; // Unpinned URL
    Ok(...)
}
```

This node claims `Stable` but doesn’t pin the resource or declare a cache; validator should emit `CACHE002`. Mitigation: use `PinnedUrl` (with version hash) or downgrade determinism.

### Example: Retry ownership conflict (`RETRY010`)

```rust
#[node(
    name = "SendEmail",
    effects = "Effectful",
    retry_owner = "Connector"
)]
async fn send_email(ctx: &mut Ctx<Params, Resources>, message: Email) -> NodeResult<()> {
    ctx.resources.sendgrid().send_with_retry(&message).await // connector has internal retry
}

workflow! {
    name: notify,
    version: "1.0.0",
    profile: Queue;
    let mailer = send_email_node_spec();
    // connect!(mailer -> sink);
    on_error!(scope = mailer, strategy = Retry { attempts = 3 }) // orchestrator retry as well
}
```

`RETRY010` should fire; mitigation is to let only one layer own retries.

### Example: Timeout budgets (`TIME015`)

```rust
#[node(name = "SlowTransform", effects = "Pure", determinism = "Strict")]
async fn slow_transform(input: Bytes) -> NodeResult<Bytes> {
    std::thread::sleep(std::time::Duration::from_secs(5)); // blocks executor
    Ok(input)
}
```

Runtime should emit `TIME015` when execution exceeds configured timeout; mitigation is to use async-friendly APIs (e.g., `tokio::time::sleep`) and respect provided deadline.

## 7. Control Surfaces & Lints
- **Missing hints** — `CTRL001` (RFC §4.9). When policies require explicit control-surface macros (`switch!`, `for_each!`), raw branching/loops generate warnings. Code pattern: using bare `match`/`for` without `#[flow::switch]`/`#[flow::for_each]` wrappers in lint-enforced contexts.
- **Invalid routing config** — `CTRL110`/`CTRL111`/`CTRL112` (switch) and `CTRL120`/`CTRL121`/`CTRL122` (if). Malformed routing surfaces, missing required `source -> target` edges, or multiple routing surfaces for the same `source` alias.
- **Human-in-the-loop misuse** — `DAG300` (RFC §4.6). Attaching `hitl!` to invalid scope.

### Example: Missing control hint (`CTRL001`)

```rust
#[node(name = "RouteOrder", effects = "Pure")]
async fn route(order: Order) -> NodeResult<RouteDecision> {
    if order.total > 500 {        // Bare conditional
        Ok(RouteDecision::Manual)
    } else {
        Ok(RouteDecision::Auto)
    }
}
```

When `policies.lint.require_control_hints` is true, validator should emit `CTRL001` suggesting `if_!`/`switch!` usage.

**Mitigation:** wrap the branch with `if_!`/`switch!` to leverage control-surface metadata.

## 8. Importer & Tooling Specific
- **Importer lossy semantics** — Warnings documented in importer output when n8n JSON can’t express required determinism/effects. This feeds into validator once IR is emitted.
- **Studio/CLI** — CLI surfaces the same diagnostics using `kernel-plan::validate`. Future subcommands (`flows run`, `flows certify`) will also emit runtime errors from harnesses and actual execution.

## Why the RFC Is “Exhaustive Enough”

The RFC enumerates these error families in multiple sections:
- §4.1 Lists compile/build-time validations for nodes/triggers.
- §4.9, §5.4 enumerate validator rules by code (`DAG*`, `IDEM*`, `CACHE*`, etc.).
- §6–§7 detail runtime/certification diagnostics.
- §8 & Appendix C cover effects/determinism resource mapping.
- §15 outlines policy/sec requirements (egress, secrets, data classification).

What remains open:
- Plumbing capability metadata into Flow IR so `EFFECT201`/`DET301` can run purely in validator (macros currently have partial knowledge).
- Static heuristics for detecting problematic Rust imports (e.g., `rand::`) beyond declared capabilities.
- Runtime harness codes (e.g., determinism replay failures) to be codified when harness implementations land.

### Action Items
- Ensure macros capture capability usage and determinism hints (per RFC §4.1) so validator rules can fire deterministically.
- Expand `kernel-plan` with the remaining checks (DAG202/203, EFFECT201, DET301, RETRY010/011, CACHE002, etc.).
- Integrate policy metadata into Flow IR (`FlowPolicies`) to allow configurable severity.
- Document any additional runtime-only codes in `impl-docs/error-codes.md` as harnesses and hosts evolve.

This taxonomy should evolve with the implementation. When adding a new diagnostic, update `impl-docs/error-codes.md`, link it back to the RFC (or document rationale if it’s a new ADR), and note the trigger patterns here.
