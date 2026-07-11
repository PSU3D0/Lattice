use super::*;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cap_http_reqwest::ReqwestHttpClient;
use capabilities::durability::{
    CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore, Lease,
};
use capabilities::kv::MemoryKv;
use capabilities::{Capability, ResourceBag};
use connector_http::runtime::transport::EnvConnectorRuntime;
use connectors_std::dev::MemoryDedupeStore;
use dag_core::requirements::TriggerKind;
use dag_core::{EffectHint, Effects, IdempotencyScope};
use host_inproc::{HostExecutionResult, HostRuntime, Invocation};
use httpmock::Method::{GET, POST};
use httpmock::MockServer;
use kernel_plan::derive_requirements;

// ---- In-memory checkpoint store (s16/s18 test double) ----------------------

#[derive(Default)]
struct MemoryCheckpointStore {
    records: Mutex<BTreeMap<String, CheckpointRecord>>,
}

impl Capability for MemoryCheckpointStore {
    fn name(&self) -> &'static str {
        "checkpoint_store.memory"
    }
}

#[async_trait]
impl CheckpointStore for MemoryCheckpointStore {
    async fn put(&self, record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError> {
        let handle = CheckpointHandle {
            checkpoint_id: record.checkpoint_id.clone(),
            flow_id: record.flow_id.clone(),
            run_id: record.run_id.clone(),
        };
        self.records
            .lock()
            .expect("records")
            .insert(record.checkpoint_id.clone(), record);
        Ok(handle)
    }

    async fn get(&self, handle: &CheckpointHandle) -> Result<CheckpointRecord, CheckpointError> {
        self.records
            .lock()
            .expect("records")
            .get(&handle.checkpoint_id)
            .cloned()
            .ok_or(CheckpointError::NotFound)
    }

    async fn ack(&self, handle: &CheckpointHandle) -> Result<(), CheckpointError> {
        self.records
            .lock()
            .expect("records")
            .remove(&handle.checkpoint_id);
        Ok(())
    }

    async fn lease(
        &self,
        handle: &CheckpointHandle,
        ttl: Duration,
    ) -> Result<Lease, CheckpointError> {
        Ok(Lease {
            lease_id: format!("lease:{}", handle.checkpoint_id),
            expires_at_ms: ttl.as_millis().try_into().unwrap_or(u64::MAX),
        })
    }

    async fn release_lease(&self, _lease: Lease) -> Result<(), CheckpointError> {
        Ok(())
    }

    async fn list(
        &self,
        _filter: CheckpointFilter,
    ) -> Result<Vec<CheckpointHandle>, CheckpointError> {
        Ok(Vec::new())
    }
}

// ---- Env plumbing ----------------------------------------------------------

const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_HTTP_TARGET_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_HTTP_TARGET_AUTH";

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

// ---- Fixtures --------------------------------------------------------------

fn ingest_event() -> serde_json::Value {
    serde_json::json!({ "sku": "SKU-42", "event_id": "evt-001" })
}

fn product_json() -> serde_json::Value {
    serde_json::json!({
        "sku": "SKU-42",
        "name": "Widget",
        "price_cents": 1999,
        "in_stock": true
    })
}

// ---- Pure helpers ----------------------------------------------------------

#[test]
fn draft_is_a_pure_projection_of_the_typed_product() {
    let fetched = ProductFetched {
        event_id: "evt-001".to_string(),
        sku: "SKU-42".to_string(),
        product: Product {
            sku: "SKU-42".to_string(),
            name: "Widget".to_string(),
            price_cents: 1999,
            in_stock: true,
        },
    };
    let draft = draft_from(&fetched);
    assert_eq!(draft.price_dollars, 19.99);
    assert_eq!(draft.idempotency_key, stats_key("evt-001", "SKU-42"));
    let body = stats_body(&draft);
    assert_eq!(body["price_dollars"], serde_json::json!(19.99));
    assert_eq!(body["sku"], serde_json::json!("SKU-42"));
}

// ---- Flow validation + requirements ----------------------------------------

#[test]
fn flow_validates_and_derives_both_http_entrypoints() {
    let ir = validated_ir();
    let requirements = derive_requirements(&ir);

    for alias in [INGEST_TRIGGER, PROBE_TRIGGER] {
        let trigger = requirements
            .triggers
            .iter()
            .find(|t| t.alias == alias)
            .unwrap_or_else(|| panic!("trigger `{alias}` present"));
        assert_eq!(trigger.kind, TriggerKind::Http);
    }

    let ingest = requirements
        .entrypoints
        .iter()
        .find(|e| e.trigger_alias == INGEST_TRIGGER)
        .expect("ingest entrypoint");
    assert_eq!(ingest.route_path.as_deref(), Some("/ingest"));
    assert_eq!(ingest.method.as_deref(), Some("POST"));
}

// ---- Flow shape: honest per-node effects + connector ops -------------------

fn node<'a>(ir: &'a dag_core::FlowIR, alias: &str) -> &'a dag_core::NodeIR {
    ir.nodes
        .iter()
        .find(|n| n.alias == alias)
        .unwrap_or_else(|| panic!("node `{alias}` present"))
}

#[test]
fn fetch_product_is_a_typed_tier0_get_with_connector_scope_from_ops() {
    let ir = flow();
    let fetch = node(&ir, "fetch_product");

    // Deliverable #7 (s15 finding, CONFIRMED): runtime bound-connection scope
    // is derived from the node IDENTIFIER (kernel_exec::infer_connector_id),
    // not from connector_ops[].connector_id. A custom node invoking a
    // connector.http op MUST therefore carry a `connector.http.<surface>`
    // identifier or the connection fails to resolve at runtime.
    assert_eq!(fetch.identifier, "connector.http.fetch_product");
    assert!(
        fetch.identifier.starts_with("connector.http."),
        "identifier must encode the connector_id for runtime scope resolution"
    );
    // connector_ops still records the op for the lock/preflight path.
    let op = fetch
        .connector_ops
        .first()
        .expect("fetch_product carries a connector op");
    assert_eq!(op.connector_id, "connector.http");
    assert_eq!(op.operation_id, "connector.http.get");

    // GET is ReadOnly + http_read.
    assert!(
        fetch
            .effect_hints
            .contains(&EffectHint::HttpRead.as_str().to_string())
    );
    assert!(
        !fetch
            .effect_hints
            .contains(&EffectHint::HttpWrite.as_str().to_string())
    );
}

#[test]
fn push_stats_is_effectful_write_with_the_dedupe_idempotency_declaration() {
    let ir = flow();
    let push = node(&ir, "push_stats");

    assert_eq!(push.effects, Effects::Effectful);
    // http_write from the POST op; kv hints for the dedupe binding.
    assert!(
        push.effect_hints
            .contains(&EffectHint::HttpWrite.as_str().to_string())
    );
    assert!(
        push.effect_hints
            .contains(&EffectHint::KvWrite.as_str().to_string())
    );
    assert_eq!(
        push.connector_ops.first().unwrap().operation_id,
        "connector.http.post"
    );

    // §6 idempotency declaration: key + scope + TTL on the NodeSpec.
    assert_eq!(push.idempotency.key.as_deref(), Some("idempotency_key"));
    assert_eq!(push.idempotency.scope, Some(IdempotencyScope::Node));
    assert_eq!(push.idempotency.ttl_ms, Some(IDEMPOTENCY_TTL_MS));
}

#[test]
fn webhook_ping_is_a_get_declared_effectful_the_section4_rule() {
    let ir = flow();
    let ping = node(&ir, "webhook_ping");

    // The node is declared Effectful...
    assert_eq!(ping.effects, Effects::Effectful);
    // ...but the op's hint stays http::read (it IS a GET) — never http::write.
    assert_eq!(
        ping.connector_ops.first().unwrap().operation_id,
        "connector.http.get"
    );
    assert!(
        ping.effect_hints
            .contains(&EffectHint::HttpRead.as_str().to_string()),
        "the webhook GET keeps its http::read hint"
    );
    assert!(
        !ping
            .effect_hints
            .contains(&EffectHint::HttpWrite.as_str().to_string()),
        "a GET declared Effectful must NOT masquerade as http::write"
    );
    // It opts into the write-side idempotency obligation.
    assert_eq!(ping.idempotency.key.as_deref(), Some("event_id"));
    assert_eq!(ping.idempotency.ttl_ms, Some(IDEMPOTENCY_TTL_MS));
}

#[test]
fn probe_public_is_a_tier2_any_origin_get_with_no_auth() {
    let ir = flow();
    let probe = node(&ir, "probe_public");
    assert_eq!(probe.identifier, "connector.http.get_any_origin");
    assert!(
        probe
            .effect_hints
            .contains(&EffectHint::HttpRead.as_str().to_string())
    );

    // The op's roles: an any_origin endpoint grant and NO outbound-auth role
    // (spec §2/§7 — auth × dynamic host is forbidden).
    let roles = connector_http::ops::HttpGetAnyOrigin::META.roles;
    assert!(
        roles
            .iter()
            .any(|r| r.expected_handle_kind == "endpoint.any_origin"),
        "Tier-2 op must require an any_origin endpoint grant"
    );
    assert!(
        !roles
            .iter()
            .any(|r| matches!(r.kind, dag_core::ConnectorRoleKindDecl::OutboundAuth)),
        "Tier-2 op must NOT declare an outbound-auth role"
    );
}

// ---- Tier-2 SSRF guard (spec §10): https-only, loopback denylisted ---------

#[test]
fn tier2_ssrf_guard_rejects_loopback_and_http_allows_public_https() {
    use connector_http::runtime::http_api::ssrf_guard;
    // http scheme rejected.
    assert!(ssrf_guard("http://example.com/x").is_err());
    // loopback / RFC1918 / metadata rejected.
    assert!(ssrf_guard("https://127.0.0.1/x").is_err());
    assert!(ssrf_guard("https://localhost/x").is_err());
    assert!(ssrf_guard("https://169.254.169.254/latest").is_err());
    assert!(ssrf_guard("https://10.0.0.1/x").is_err());
    // A public https origin passes the static guard.
    assert!(ssrf_guard("https://api.public.example/v1/status").is_ok());
}

// ---- End-to-end main flow: typed GET + dedupe POST + Effectful GET ---------

fn main_bag(kv: Arc<MemoryKv>) -> ResourceBag {
    let client = Arc::new(ReqwestHttpClient::default());
    ResourceBag::default()
        .with_http_read(Arc::clone(&client))
        .with_http_write(client)
        .with_connector_runtime(Arc::new(EnvConnectorRuntime))
        .with_checkpoint_store(Arc::new(MemoryCheckpointStore::default()))
        .with_kv(kv)
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn ingest_typed_gets_shapes_dedupe_posts_and_fires_the_webhook() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "s26-http-token");

    let product = server.mock(|when, then| {
        when.method(GET)
            .path("/v2/products/SKU-42")
            .header("authorization", "Bearer s26-http-token");
        then.status(200).json_body_obj(&product_json());
    });
    let stats = server.mock(|when, then| {
        when.method(POST).path("/v2/stats");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "id": "stat-1", "ok": true }));
    });
    let notify = server.mock(|when, then| {
        when.method(GET).path("/hooks/notify");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "ok": true }));
    });

    let kv = Arc::new(MemoryKv::new());
    let bundle = bundle();
    let runtime = HostRuntime::new(bundle.executor(), Arc::new(bundle.validated_ir))
        .with_resource_bag(main_bag(kv));

    let fire = || Invocation::new(INGEST_TRIGGER, "capture", ingest_event());

    // First delivery: typed decode succeeds, POST fires, webhook fires.
    let first = match runtime.execute(fire()).await.expect("first delivery runs") {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<IngestReceipt>(value).expect("decode receipt")
        }
        _ => panic!("expected a value result"),
    };
    assert!(first.push.applied, "first POST applies");
    assert_eq!(first.push.remote_id, "stat-1");
    assert!(first.ping.fired, "first webhook fires");

    // Redelivery of the SAME event: POST + webhook are deduped (no double-fire).
    let second = match runtime.execute(fire()).await.expect("redelivery runs") {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<IngestReceipt>(value).expect("decode receipt")
        }
        _ => panic!("expected a value result"),
    };
    assert!(!second.push.applied, "redelivery must NOT re-POST");
    assert!(
        !second.ping.fired,
        "redelivery must NOT re-fire the webhook"
    );

    // The read GET reran (reads are safe to replay); the write POST and the
    // Effectful webhook GET each fired exactly once across two deliveries.
    product.assert_hits(2);
    stats.assert_hits(1);
    notify.assert_hits(1);
}

// ---- §5 typed mode is fail-closed: a shape mismatch errors -----------------

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn typed_decode_failure_fails_the_flow_closed() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    // 2xx JSON that does not match `Product` (price_cents missing/typed wrong).
    let _product = server.mock(|when, then| {
        when.method(GET).path("/v2/products/SKU-42");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "sku": "SKU-42", "name": "Widget" }));
    });

    let kv = Arc::new(MemoryKv::new());
    let bundle = bundle();
    let runtime = HostRuntime::new(bundle.executor(), Arc::new(bundle.validated_ir))
        .with_resource_bag(main_bag(kv));

    let result = runtime
        .execute(Invocation::new(INGEST_TRIGGER, "capture", ingest_event()))
        .await;
    let failed = match result {
        Err(_) => true,
        Ok(HostExecutionResult::Value(_)) => false,
        Ok(_) => true,
    };
    assert!(
        failed,
        "typed<Product> decode failure must fail the flow closed, not fall back to raw"
    );
}

// ---- Normative §6 exactly-once edge + dedupe binding (exactly_once module) --

#[test]
fn exactly_once_flow_validates_with_the_normative_edge() {
    // If EXACT001-003 fired (missing dedupe binding / key / TTL), validate()
    // would return diagnostics and validated_ir() would panic.
    let ir = exactly_once::flow();
    let validated = kernel_plan::validate(&ir);
    assert!(
        validated.is_ok(),
        "exactly-once edge must satisfy EXACT001-003: {:?}",
        validated.err()
    );
    let edge = ir
        .edges
        .iter()
        .find(|e| e.from == "seed" && e.to == "commit")
        .expect("seed -> commit edge");
    assert_eq!(edge.delivery, dag_core::Delivery::ExactlyOnce);

    let commit = node(&ir, "commit");
    assert!(
        commit
            .effect_hints
            .contains(&EffectHint::DedupeWrite.as_str().to_string()),
        "commit must carry the dedupe binding"
    );
    assert_eq!(commit.idempotency.scope, Some(IdempotencyScope::Edge));
    assert_eq!(
        commit.idempotency.ttl_ms,
        Some(exactly_once::EXACTLY_ONCE_TTL_MS)
    );
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn exactly_once_edge_does_not_double_fire_through_a_real_dedupe_store() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let write = server.mock(|when, then| {
        when.method(POST).path("/v2/commit");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "id": "commit-1" }));
    });

    let client = Arc::new(ReqwestHttpClient::default());
    let bag = ResourceBag::default()
        .with_http_read(Arc::clone(&client))
        .with_http_write(client)
        .with_connector_runtime(Arc::new(EnvConnectorRuntime))
        .with_checkpoint_store(Arc::new(MemoryCheckpointStore::default()))
        .with_dedupe(Arc::new(MemoryDedupeStore::new()));

    let bundle = exactly_once::bundle();
    let runtime =
        HostRuntime::new(bundle.executor(), Arc::new(bundle.validated_ir)).with_resource_bag(bag);

    let payload = serde_json::json!({
        "idempotency_key": "commit-key-1",
        "path": "/v2/commit",
        "body": { "amount": 100 }
    });
    let fire = || Invocation::new("seed", "commit_capture", payload.clone());

    let first = match runtime.execute(fire()).await.expect("first commit runs") {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<exactly_once::CommitResult>(value).expect("decode")
        }
        _ => panic!("expected a value result"),
    };
    assert!(first.applied, "first exactly-once write applies");

    let second = match runtime.execute(fire()).await.expect("redelivery runs") {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<exactly_once::CommitResult>(value).expect("decode")
        }
        _ => panic!("expected a value result"),
    };
    assert!(!second.applied, "redelivery must NOT re-write");

    write.assert_hits(1);
}
