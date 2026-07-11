//! S26 — the generic HTTP long-tail acceptance flow (packet H4 of
//! `impl-docs/spec/http-request-node.md`). This example proves the whole
//! `connector.http` path end-to-end and is the canonical clone-recipe
//! reference for future farming agents.
//!
//! Behavioral spec (independent Rust implementation; no third-party workflow
//! content is embedded here). A generic REST API — the kind that will never
//! get a Lattice connector family — is served through `connector.http` on
//! every path that matters:
//!
//! 1. An **HTTP webhook** (`POST /ingest`) receives a product event
//!    `{ sku, event_id }`.
//! 2. **`http_get` (Tier 0, typed<T>)** fetches `/v2/products/{sku}` and
//!    serde-deserializes the 2xx JSON straight into a Rust `Product` struct —
//!    the spec §5 typed mode, fail-closed on decode error. This runs inside a
//!    custom `def_node` (`fetch_product`) whose identifier is NOT
//!    connector-prefixed, exercising the s15 scope-inference finding: the
//!    node's bound-connection scope resolves through
//!    `connector_ops[].connector_id` (`connector.http`), not the identifier.
//! 3. A **pure shape node** (`shape`) projects the typed response into the
//!    outbound stats draft and derives the idempotency key.
//! 4. **`http_post` (Tier 0, Effectful)** (`push_stats`) writes the stats to
//!    `/v2/stats`, guarded by the §6 dedupe binding: a KV `get`/`put`
//!    idempotency record keyed on `event_id` with a TTL. A redelivery of the
//!    same event does NOT double-fire the POST (s16/s18 pattern).
//! 5. **A webhook-style GET declared Effectful** (`webhook_ping`) fires a
//!    trigger URL (`GET /hooks/notify`). This is the spec §4 rule: a GET whose
//!    semantic purpose is to fire an action MUST be declared
//!    `effects = Effectful` on the node (opting it into the write-side
//!    idempotency obligations) even though the op's hint stays `http::read`.
//! 6. A terminal capture returns the receipt.
//!
//! A second entrypoint (`POST /probe`) drives a **Tier-2 unauthenticated GET**
//! (`get_any_origin`) against a runtime-supplied absolute URL — the any-origin
//! path, which carries no lock-granted credential (spec §2/§7). The SSRF guard
//! (§10) keeps it https-only with a loopback/metadata denylist, so it is
//! exercised through the connector's public `ssrf_guard` in the tests rather
//! than against the loopback mock.
//!
//! The normative §6 **exactly-once edge + dedupe binding** mechanism (the real
//! `Delivery::ExactlyOnce` + `DedupeStore` wiring, EXACT001–003) is
//! demonstrated in the [`exactly_once`] submodule, proven end-to-end with a
//! `MemoryDedupeStore`. The main flow above uses the KV idempotency pattern
//! (s16/s18) because the CLI's lock-provisioned resource bag does not mint a
//! dedupe store; both patterns satisfy "a redelivery does not double-fire".

use std::time::Duration;

use capabilities::context;
use connector_http::ops::{HttpGet, HttpPost};
use connector_http::{HttpJsonOutput, HttpReadAnyOriginInput, HttpReadInput, HttpWriteInput};
use dag_core::{FlowIR, NodeError, NodeResult};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub mod exactly_once;

pub const FLOW_NAME: &str = "s26_http_longtail_flow";
pub const INGEST_TRIGGER: &str = "intake";
pub const PROBE_TRIGGER: &str = "probe_trigger";

/// Dedupe TTL for the KV idempotency records (24h). A redelivery inside the
/// window collapses to the first delivery's effect.
pub const IDEMPOTENCY_TTL_MS: u64 = 86_400_000;

// ---------------------------------------------------------------------------
// Boundary types
// ---------------------------------------------------------------------------

/// Webhook payload: a product event to ingest.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IngestEvent {
    pub sku: String,
    pub event_id: String,
}

/// The typed target of the Tier-0 GET (spec §5 `typed<T>`). serde decodes the
/// 2xx JSON body straight into this struct; a decode mismatch is a NodeError
/// (`HTTP103`), never a silent fallback.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Product {
    pub sku: String,
    pub name: String,
    pub price_cents: u64,
    pub in_stock: bool,
}

/// Output of the typed fetch: the decoded product plus the ids the rest of the
/// flow needs to stay a pure function of the inbound event.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProductFetched {
    pub event_id: String,
    pub sku: String,
    pub product: Product,
}

/// Pure projection of the typed response into the outbound stats draft.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StatsDraft {
    /// The stats-write idempotency key (spec §6 idempotency key).
    pub idempotency_key: String,
    pub event_id: String,
    pub sku: String,
    pub name: String,
    pub price_dollars: f64,
    pub in_stock: bool,
}

/// What the dedupe-guarded POST did for this delivery.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PushResult {
    pub idempotency_key: String,
    /// `false` on a redelivery whose key was already recorded.
    pub applied: bool,
    pub sku: String,
    pub remote_id: String,
}

/// Carries the POST result forward alongside the ids the webhook ping needs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Pushed {
    pub event_id: String,
    pub sku: String,
    pub push: PushResult,
}

/// What the Effectful webhook GET did for this delivery.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PingResult {
    pub idempotency_key: String,
    /// `false` on a redelivery whose key was already recorded.
    pub fired: bool,
    pub sku: String,
}

/// Terminal receipt.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IngestReceipt {
    pub sku: String,
    pub event_id: String,
    pub push: PushResult,
    pub ping: PingResult,
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

/// Idempotency key for the stats write (spec shape `<flow>:stats:{event}:{sku}`).
pub fn stats_key(event_id: &str, sku: &str) -> String {
    format!("{FLOW_NAME}:stats:{event_id}:{sku}")
}

/// Idempotency key for the webhook ping.
pub fn ping_key(event_id: &str, sku: &str) -> String {
    format!("{FLOW_NAME}:ping:{event_id}:{sku}")
}

/// Project the typed product into the outbound stats draft.
pub fn draft_from(fetched: &ProductFetched) -> StatsDraft {
    StatsDraft {
        idempotency_key: stats_key(&fetched.event_id, &fetched.sku),
        event_id: fetched.event_id.clone(),
        sku: fetched.sku.clone(),
        name: fetched.product.name.clone(),
        price_dollars: fetched.product.price_cents as f64 / 100.0,
        in_stock: fetched.product.in_stock,
    }
}

/// The JSON body of the stats write — a pure function of the draft.
pub fn stats_body(draft: &StatsDraft) -> serde_json::Value {
    json!({
        "sku": draft.sku,
        "name": draft.name,
        "price_dollars": draft.price_dollars,
        "in_stock": draft.in_stock,
        "idempotency_key": draft.idempotency_key,
    })
}

/// Read-through-then-reserve against KV: returns `true` if this key had not
/// been recorded yet (the caller should perform the effect), `false` on a
/// redelivery. The record carries a TTL (spec §6). This is the s16/s18 dedupe
/// binding.
async fn reserve(key: &str, payload: serde_json::Value) -> NodeResult<bool> {
    let key = key.to_string();
    context::with_current_async(|resources| {
        let key = key.clone();
        async move {
            let kv = resources.kv().ok_or_else(|| {
                NodeError::new("this node requires a KV capability (declare resource::kv::write)")
            })?;
            if kv
                .get(&key)
                .await
                .map_err(|err| node_error(format!("kv get failed: {err}")))?
                .is_some()
            {
                return Ok::<bool, NodeError>(false);
            }
            let row = serde_json::to_vec(&payload)
                .map_err(|err| node_error(format!("serialize dedupe record: {err}")))?;
            kv.put(&key, &row, Some(Duration::from_millis(IDEMPOTENCY_TTL_MS)))
                .await
                .map_err(|err| node_error(format!("kv put failed: {err}")))?;
            Ok(true)
        }
    })
    .await
    .ok_or_else(|| NodeError::new("missing ResourceAccess context"))?
}

// ---------------------------------------------------------------------------
// Nodes — entrypoint 1 (`POST /ingest`)
// ---------------------------------------------------------------------------

/// Webhook ingress: passes the typed event through.
#[def_node(
    trigger,
    name = "Intake",
    summary = "HTTP webhook ingress; receives the product event",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn intake(event: IngestEvent) -> NodeResult<IngestEvent> {
    Ok(event)
}

/// Tier-0 typed GET (spec §5 `typed<T>`).
///
/// Deliverable #7 / the s15 scope-inference finding, CONFIRMED by packet H4:
/// the runtime derives a node's bound-connection scope from its **identifier**
/// (`kernel_exec::infer_connector_id`: a `connector.<id>.<surface>` identifier
/// yields `connector_id = connector.<id>`), NOT from
/// `connector_ops[].connector_id`. A custom node invoking a `connector.http`
/// op therefore MUST carry a `connector.http.<surface>` identifier, or the
/// bound connection fails to resolve at runtime (`no connector binding
/// resolved for connector ...`). `connector_ops(...)` still drives the
/// lock-recording/preflight path, but the identifier is load-bearing for
/// execution.
#[def_node(
    name = "FetchProduct",
    identifier = "connector.http.fetch_product",
    summary = "Fetch and typed-decode the product via connector.http.get",
    connector_ops(connector_http::ops::HttpGet)
)]
async fn fetch_product(event: IngestEvent) -> NodeResult<ProductFetched> {
    let product: Product = HttpGet::invoke_typed(&HttpReadInput {
        path: format!("/v2/products/{}", event.sku),
        ..Default::default()
    })
    .await
    .map_err(|err| node_error(format!("connector.http.get (typed Product) failed: {err}")))?;

    Ok(ProductFetched {
        event_id: event.event_id,
        sku: event.sku,
        product,
    })
}

/// Pure projection of the typed response.
#[def_node(
    name = "Shape",
    summary = "Project the typed product into the outbound stats draft",
    effects = "Pure",
    determinism = "Strict"
)]
async fn shape(fetched: ProductFetched) -> NodeResult<StatsDraft> {
    Ok(draft_from(&fetched))
}

/// Tier-0 Effectful POST with the §6 dedupe binding: a KV idempotency record
/// keyed on `event_id`, with a TTL, so a redelivery does not double-fire the
/// write. The `idempotency(...)` attribute records the key/scope/TTL on the
/// NodeSpec for the planner.
#[def_node(
    name = "PushStats",
    identifier = "connector.http.push_stats",
    summary = "POST the stats via connector.http.post, guarded by a KV idempotency record",
    effects = "Effectful",
    determinism = "BestEffort",
    connector_ops(connector_http::ops::HttpPost),
    resources(
        kv_read(capabilities::kv::KeyValue),
        kv_write(capabilities::kv::KeyValue)
    ),
    idempotency(key = "idempotency_key", scope = "Node", ttl_ms = 86_400_000)
)]
async fn push_stats(draft: StatsDraft) -> NodeResult<Pushed> {
    let key = draft.idempotency_key.clone();
    let first = reserve(
        &key,
        json!({ "kind": "stats", "sku": draft.sku, "event_id": draft.event_id }),
    )
    .await?;

    let push = if first {
        let out: HttpJsonOutput = HttpPost::invoke(&HttpWriteInput {
            path: "/v2/stats".to_string(),
            body: Some(stats_body(&draft)),
            ..Default::default()
        })
        .await
        .map_err(|err| node_error(format!("connector.http.post failed: {err}")))?;
        let remote_id = out
            .body
            .get("id")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string();
        PushResult {
            idempotency_key: key,
            applied: true,
            sku: draft.sku.clone(),
            remote_id,
        }
    } else {
        PushResult {
            idempotency_key: key,
            applied: false,
            sku: draft.sku.clone(),
            remote_id: String::new(),
        }
    };

    Ok(Pushed {
        event_id: draft.event_id,
        sku: draft.sku,
        push,
    })
}

/// The spec §4 rule: a webhook-style GET declared **Effectful**. The op hint
/// stays `http::read` (it is a GET), but the node's semantic purpose is to fire
/// an action, so the node declares `effects = "Effectful"` and carries the same
/// write-side idempotency obligation as a write. Declaring it ReadOnly would
/// make replays "safe by declaration" — a quiet lie the recipe forbids.
#[def_node(
    name = "WebhookPing",
    identifier = "connector.http.webhook_ping",
    summary = "Fire a trigger URL via connector.http.get, declared Effectful (spec §4)",
    effects = "Effectful",
    determinism = "BestEffort",
    connector_ops(connector_http::ops::HttpGet),
    resources(
        kv_read(capabilities::kv::KeyValue),
        kv_write(capabilities::kv::KeyValue)
    ),
    idempotency(key = "event_id", scope = "Node", ttl_ms = 86_400_000)
)]
async fn webhook_ping(pushed: Pushed) -> NodeResult<IngestReceipt> {
    let key = ping_key(&pushed.event_id, &pushed.sku);
    let first = reserve(
        &key,
        json!({ "kind": "ping", "sku": pushed.sku, "event_id": pushed.event_id }),
    )
    .await?;

    if first {
        // A GET that FIRES an action (the honest reason this node is Effectful).
        HttpGet::invoke(&HttpReadInput {
            path: "/hooks/notify".to_string(),
            query: vec![("sku".to_string(), pushed.sku.clone())],
            ..Default::default()
        })
        .await
        .map_err(|err| node_error(format!("connector.http.get (webhook ping) failed: {err}")))?;
    }

    Ok(IngestReceipt {
        sku: pushed.sku.clone(),
        event_id: pushed.event_id,
        push: pushed.push,
        ping: PingResult {
            idempotency_key: key,
            fired: first,
            sku: pushed.sku,
        },
    })
}

/// Terminal capture.
#[def_node(
    name = "Capture",
    summary = "Return the ingest receipt",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(receipt: IngestReceipt) -> NodeResult<IngestReceipt> {
    Ok(receipt)
}

// ---------------------------------------------------------------------------
// Nodes — entrypoint 2 (`POST /probe`): the Tier-2 any-origin path
// ---------------------------------------------------------------------------

/// Seeds the Tier-2 GET input: a runtime-supplied absolute URL.
#[def_node(
    trigger,
    name = "ProbeTrigger",
    summary = "HTTP webhook ingress; seeds the Tier-2 any-origin URL",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn probe_trigger(input: HttpReadAnyOriginInput) -> NodeResult<HttpReadAnyOriginInput> {
    Ok(input)
}

/// Passes the Tier-2 JSON response through.
#[def_node(
    name = "ProbeCapture",
    summary = "Return the any-origin GET response unchanged",
    effects = "Pure",
    determinism = "Strict"
)]
async fn probe_capture(response: HttpJsonOutput) -> NodeResult<HttpJsonOutput> {
    Ok(response)
}

// ---------------------------------------------------------------------------
// Flow
// ---------------------------------------------------------------------------

// The flow declares two HTTP triggers (the `/ingest` pipeline and the `/probe`
// Tier-2 seam), so it opts into `allow_multiple_triggers` after macro
// expansion — the s7 pattern. The `flow!` macro is wrapped in a private module
// and the crate-root `flow()`/`validated_ir()`/`bundle()` patch the policy
// before validation.
mod bundle_def {
    use dag_macros::node;

    dag_macros::flow! {
        name: s26_http_longtail_flow,
        version: "1.0.0",
        profile: Web,
        summary: "H4 acceptance: generic connector.http on every path — typed Tier-0 GET, pure shape, dedupe-guarded Effectful POST, an Effectful webhook GET (spec §4), and a Tier-2 unauthenticated any-origin GET";

        // Entrypoint 1: POST /ingest.
        let intake = node!(intake);
        let fetch_product = node!(fetch_product);
        let shape = node!(shape);
        let push_stats = node!(push_stats);
        let webhook_ping = node!(webhook_ping);
        let capture = node!(capture);

        connect!(intake -> fetch_product);
        connect!(fetch_product -> shape);
        connect!(shape -> push_stats);
        connect!(push_stats -> webhook_ping);
        connect!(webhook_ping -> capture);

        // Entrypoint 2: POST /probe — the Tier-2 any-origin GET.
        let probe_trigger = node!(probe_trigger);
        let probe_public = node!(connector_http::http_get_any_origin);
        let probe_capture = node!(probe_capture);

        connect!(probe_trigger -> probe_public);
        connect!(probe_public -> probe_capture);

        entrypoint!({
            trigger: "intake",
            capture: "capture",
            route_aliases: ["/ingest"],
            method: "POST",
            deadline_ms: 15_000,
        });
        entrypoint!({
            trigger: "probe_trigger",
            capture: "probe_capture",
            route_aliases: ["/probe"],
            method: "POST",
            deadline_ms: 10_000,
        });
    }
}

/// The flow IR, with the multi-trigger lint opt-in applied.
pub fn flow() -> FlowIR {
    let mut flow = bundle_def::flow();
    flow.policies.lint.allow_multiple_triggers = Some(true);
    flow
}

/// The validated IR.
pub fn validated_ir() -> kernel_plan::ValidatedIR {
    kernel_plan::validate(&flow()).expect("s26 flow should validate")
}

/// The host-inproc bundle (both entrypoints intact).
#[cfg(feature = "host-bundle")]
pub fn bundle() -> host_inproc::FlowBundle {
    use std::sync::Arc;

    use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
    use kernel_exec::NodeRegistry;

    let validated_ir = validated_ir();
    let mut registry = NodeRegistry::new();
    intake_register(&mut registry).expect("register intake");
    fetch_product_register(&mut registry).expect("register fetch_product");
    shape_register(&mut registry).expect("register shape");
    push_stats_register(&mut registry).expect("register push_stats");
    webhook_ping_register(&mut registry).expect("register webhook_ping");
    capture_register(&mut registry).expect("register capture");
    probe_trigger_register(&mut registry).expect("register probe_trigger");
    connector_http::http_get_any_origin_register(&mut registry).expect("register probe_public");
    probe_capture_register(&mut registry).expect("register probe_capture");

    let registry = Arc::new(registry);
    let resolver: Arc<dyn kernel_exec::NodeResolver> =
        Arc::new(kernel_exec::RegistryResolver::new(registry.clone()));

    let entrypoints = vec![
        FlowEntrypoint {
            trigger_alias: "intake".to_string(),
            capture_alias: "capture".to_string(),
            route_path: Some("/ingest".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(15_000)),
            route_aliases: vec!["/ingest".to_string()],
            schedule: None,
        },
        FlowEntrypoint {
            trigger_alias: "probe_trigger".to_string(),
            capture_alias: "probe_capture".to_string(),
            route_path: Some("/probe".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(10_000)),
            route_aliases: vec!["/probe".to_string()],
            schedule: None,
        },
    ];

    let node_contracts = vec![
        node!(intake),
        node!(fetch_product),
        node!(shape),
        node!(push_stats),
        node!(webhook_ping),
        node!(capture),
        node!(probe_trigger),
        node!(connector_http::http_get_any_origin),
        node!(probe_capture),
    ]
    .into_iter()
    .map(|spec| NodeContract {
        identifier: spec.identifier.to_string(),
        contract_hash: None,
        source: NodeSource::Local,
    })
    .collect();

    FlowBundle {
        validated_ir,
        entrypoints,
        resolver,
        node_contracts,
        environment_plugins: Vec::new(),
    }
}

#[cfg(test)]
mod tests;
