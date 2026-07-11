use super::*;
use std::sync::{Arc, Mutex};

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use cap_http_reqwest::ReqwestHttpClient;
use capabilities::durability::{
    CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore, Lease,
};
use capabilities::kv::{KeyValue, MemoryKv};
use capabilities::scoped::ScopedResources;
use capabilities::{Capability, ResourceAccess, ResourceBag, context};
use connector_telegram::runtime::transport::EnvConnectorRuntime;
use dag_core::EffectHint;
use dag_core::requirements::TriggerKind;
use host_inproc::{HostExecutionResult, HostRuntime, Invocation};
use httpmock::Method::{GET, POST};
use httpmock::MockServer;
use kernel_plan::derive_requirements;

// ---- In-memory checkpoint store (same shape as s16's test double) ---------

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

// ---- Env plumbing ---------------------------------------------------------

const SHEETS_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_SHEETS_DEFAULT_BASE_URL";
const TELEGRAM_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_TELEGRAM_BOT_DEFAULT_BASE_URL";
const GOOGLE_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";
const TELEGRAM_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_TELEGRAM_BOT_AUTH";

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

fn sample_request(broadcast_id: &str) -> BroadcastRequest {
    BroadcastRequest {
        broadcast_id: broadcast_id.to_string(),
        spreadsheet_id: "roster-doc".to_string(),
        sheet: "contacts".to_string(),
        message: "Release 4.2 ships Friday — reply STOP to opt out.".to_string(),
    }
}

fn row_match(chat_id: &str) -> GoogleSheetsRowMatch {
    GoogleSheetsRowMatch {
        row_number: 2,
        values: json!({ ROSTER_HEADER: chat_id }),
    }
}

// ---- Pure helpers ----------------------------------------------------------

#[test]
fn chat_ids_projects_column_dropping_blanks_and_dupes() {
    let items = vec![
        row_match("1001"),
        row_match("2002"),
        row_match("1001"),
        GoogleSheetsRowMatch {
            row_number: 5,
            values: json!({ ROSTER_HEADER: "" }),
        },
        GoogleSheetsRowMatch {
            row_number: 6,
            values: json!({ "other": "x" }),
        },
        GoogleSheetsRowMatch {
            row_number: 7,
            values: json!({ ROSTER_HEADER: 3003 }),
        },
    ];
    assert_eq!(
        chat_ids_from_matches(&items),
        vec!["1001".to_string(), "2002".to_string(), "3003".to_string()]
    );
}

#[test]
fn broadcast_key_is_stable_shape() {
    assert_eq!(
        broadcast_key("bcast-7"),
        format!("{FLOW_NAME}:{TRIGGER_ALIAS}:bcast-7")
    );
}

// ---- Flow validation + requirements ----------------------------------------

#[test]
fn flow_validates_and_derives_manual_http_entrypoint() {
    let ir = validated_ir();
    let requirements = derive_requirements(&ir);

    let trigger = requirements
        .triggers
        .iter()
        .find(|t| t.alias == TRIGGER_ALIAS)
        .expect("broadcast_trigger requirement present");
    assert_eq!(trigger.kind, TriggerKind::Http);

    let entrypoint = requirements
        .entrypoints
        .iter()
        .find(|e| e.trigger_alias == TRIGGER_ALIAS)
        .expect("http entrypoint requirement present");
    assert_eq!(entrypoint.schedule, None);
    assert_eq!(entrypoint.capture_alias, "capture");
}

#[test]
fn flow_shape_declares_honest_effects_per_node() {
    let ir = flow();

    let hints = |alias: &str| -> Vec<String> {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .unwrap_or_else(|| panic!("node `{alias}` present"))
            .effect_hints
            .clone()
    };

    // Read-only sheet roster read: http_read only.
    assert!(hints("load_roster").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(!hints("load_roster").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // Telegram send: http_write only.
    assert!(hints("broadcast_messages").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("broadcast_messages").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Terminal KV upsert.
    assert!(hints("record_broadcast").contains(&EffectHint::KvWrite.as_str().to_string()));

    // Connector nodes carry connector-prefixed identifiers so the runtime can
    // infer each connection scope (sheets vs telegram).
    let identifier = |alias: &str| {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .map(|n| n.identifier.clone())
            .expect("node present")
    };
    assert_eq!(
        identifier("load_roster"),
        "connector.google.sheets.load_roster"
    );
    assert_eq!(
        identifier("broadcast_messages"),
        "connector.telegram.broadcast_messages"
    );
}

// ---- record_broadcast honesty + idempotency --------------------------------

fn kv_bag() -> (Arc<MemoryKv>, Arc<dyn ResourceAccess>) {
    let kv = Arc::new(MemoryKv::new());
    let bag: Arc<dyn ResourceAccess> = Arc::new(ResourceBag::default().with_kv(kv.clone()));
    (kv, bag)
}

fn sample_result(broadcast_id: &str) -> BroadcastResult {
    BroadcastResult {
        request: sample_request(broadcast_id),
        recipients: 2,
        deliveries: vec![
            Delivery {
                chat_id: "1001".to_string(),
                message_id: 11,
            },
            Delivery {
                chat_id: "2002".to_string(),
                message_id: 12,
            },
        ],
    }
}

#[tokio::test]
async fn record_broadcast_succeeds_under_exactly_declared_kv_hints() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_broadcast",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let record = context::with_resources(view, async {
        record_broadcast(sample_result("bcast-1"))
            .await
            .expect("write succeeds under declared hints")
    })
    .await;

    assert!(record.stored);
    assert_eq!(record.recipients, 2);
    assert_eq!(record.delivered, 2);
    assert!(
        scoped.take_denials().is_empty(),
        "declared hints must be sufficient: no CAP110 denials"
    );
}

#[tokio::test]
async fn record_broadcast_denied_without_kv_grant() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new("record_broadcast", bag, []));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        record_broadcast(sample_result("bcast-1"))
            .await
            .expect_err("write must fail when kv is not granted")
    })
    .await;
    assert!(
        err.to_string().contains("KV capability"),
        "expected a missing-kv error, got: {err}"
    );

    let denials = scoped.take_denials();
    assert!(
        denials.iter().any(|d| d.capability == "kv"),
        "expected a CAP110 kv denial, got: {denials:?}"
    );
}

#[tokio::test]
async fn record_broadcast_is_idempotent_on_broadcast_id() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_broadcast",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));

    let first = context::with_resources(scoped.clone(), async {
        record_broadcast(sample_result("bcast-1"))
            .await
            .expect("first")
    })
    .await;
    let second = context::with_resources(scoped.clone(), async {
        record_broadcast(sample_result("bcast-1"))
            .await
            .expect("redelivery")
    })
    .await;

    assert!(first.stored, "first delivery writes the record");
    assert!(!second.stored, "redelivery is deduplicated");
    assert_eq!(first.key, second.key);

    let other = context::with_resources(scoped.clone(), async {
        record_broadcast(sample_result("bcast-2"))
            .await
            .expect("distinct broadcast")
    })
    .await;
    assert!(other.stored, "a distinct broadcast writes its own record");
    assert_ne!(first.key, other.key);
}

// ---- Compose: request -> roster read -> per-chat sends -> record -----------

// The env lock is held across awaits deliberately: the process-global
// endpoint/auth env vars must not be mutated by a parallel test mid-run.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn manual_broadcast_reads_roster_sends_each_and_records_idempotently() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _sheets = EnvGuard::set(SHEETS_ENDPOINT_ENV, &server.base_url());
    let _telegram = EnvGuard::set(TELEGRAM_ENDPOINT_ENV, &server.base_url());
    let _google_auth = EnvGuard::set(GOOGLE_AUTH_ENV, "s17-google-token");
    let _telegram_auth = EnvGuard::set(TELEGRAM_AUTH_ENV, "123456:S17TOKEN");

    // 1. sheets find_rows reads the roster tab (header row + two chat ids).
    let values_read = server.mock(|when, then| {
        when.method(GET)
            .path_contains("/values/")
            .header("authorization", "Bearer s17-google-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [["chat_id"], ["1001"], ["2002"]]
        }));
    });

    // 2. telegram send: token is in the path (/bot<token>/sendMessage).
    let telegram_send = server.mock(|when, then| {
        when.method(POST).path_contains("/sendMessage");
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": true,
            "result": { "message_id": 555, "date": 1_700_000_000 }
        }));
    });

    let kv = Arc::new(MemoryKv::new());
    let bag = ResourceBag::default()
        .with_http_read(Arc::new(ReqwestHttpClient::default()))
        .with_http_write(Arc::new(ReqwestHttpClient::default()))
        .with_connector_runtime(Arc::new(EnvConnectorRuntime))
        .with_checkpoint_store(Arc::new(MemoryCheckpointStore::default()))
        .with_kv(kv.clone());

    let bundle = bundle();
    let runtime =
        HostRuntime::new(bundle.executor(), Arc::new(bundle.validated_ir)).with_resource_bag(bag);

    let request = serde_json::to_value(sample_request("bcast-42")).expect("serialize request");
    let fire = || Invocation::new(TRIGGER_ALIAS, "capture", request.clone());

    let first = runtime.execute(fire()).await.expect("first run");
    let first = match first {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<BroadcastRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the first run"),
    };
    assert!(first.stored);
    assert_eq!(first.recipients, 2);
    assert_eq!(first.delivered, 2);
    assert_eq!(first.key, broadcast_key("bcast-42"));

    values_read.assert_hits(1);
    telegram_send.assert_hits(2);

    // Redelivery of the SAME broadcast: the roster read + sends replay, but the
    // terminal KV record dedupes to one row.
    let second = runtime.execute(fire()).await.expect("redelivery runs");
    let second = match second {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<BroadcastRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the redelivery"),
    };
    assert!(
        !second.stored,
        "redelivery must not write a duplicate record"
    );
    assert_eq!(second.key, first.key);
    telegram_send.assert_hits(4);

    let raw = kv
        .get(&broadcast_key("bcast-42"))
        .await
        .expect("kv get")
        .expect("record present");
    let row: BroadcastRecord = serde_json::from_slice(&raw).expect("decode row");
    assert_eq!(row.delivered, 2);
}
