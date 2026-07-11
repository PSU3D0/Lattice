use super::*;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cap_http_reqwest::ReqwestHttpClient;
use capabilities::durability::{
    CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore, Lease,
};
use capabilities::kv::{KeyValue, MemoryKv};
use capabilities::scoped::ScopedResources;
use capabilities::{Capability, ResourceAccess, ResourceBag, context};
use connector_airtable::runtime::transport::EnvConnectorRuntime;
use dag_core::EffectHint;
use dag_core::requirements::TriggerKind;
use host_inproc::{HostExecutionResult, HostRuntime, Invocation};
use httpmock::Method::{GET, POST};
use httpmock::MockServer;
use kernel_plan::derive_requirements;

// ---- In-memory checkpoint store (same shape as s15/s16's test double) ------

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

const AIRTABLE_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_AIRTABLE_DEFAULT_BASE_URL";
const SHEETS_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_SHEETS_DEFAULT_BASE_URL";
const NOTION_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_NOTION_DEFAULT_BASE_URL";
const AIRTABLE_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_AIRTABLE_TOKEN_AUTH";
const GOOGLE_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";
const NOTION_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_NOTION_API_AUTH";

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

const CALL_MS_START: u64 = 1_782_972_000_000; // 2026-07-02T06:00:00Z
const CALL_MS_END: u64 = 1_782_972_083_000; // +83s

fn analyzed_call() -> CallEvent {
    CallEvent {
        event: ANALYZED_EVENT.to_string(),
        call: CallPayload {
            call_id: "call-abc-001".to_string(),
            direction: "outbound".to_string(),
            from_number: "+15550000000".to_string(),
            to_number: "+15551234567".to_string(),
            start_timestamp_ms: CALL_MS_START,
            end_timestamp_ms: CALL_MS_END,
            duration_seconds: 83.0,
            transcript: "Agent: hello. User: hi.".to_string(),
            summary: "Caller booked a standard room.".to_string(),
            sentiment: "positive".to_string(),
            combined_cost_cents: 1250.0,
        },
    }
}

fn call_event_json(event: &CallEvent) -> serde_json::Value {
    serde_json::to_value(event).expect("serialize call event")
}

// ---- Pure helpers ----------------------------------------------------------

#[test]
fn utc_rfc3339_formats_timestamps() {
    assert_eq!(utc_rfc3339(0), "1970-01-01T00:00:00Z");
    assert_eq!(utc_rfc3339(CALL_MS_START), "2026-07-02T06:00:00Z");
    assert_eq!(utc_rfc3339(CALL_MS_END), "2026-07-02T06:01:23Z");
}

#[test]
fn normalize_projects_phone_by_direction_and_cost_in_dollars() {
    let outbound = normalize(&analyzed_call().call);
    // Outbound: the "to" number is the call's number.
    assert_eq!(outbound.phone_number, "+15551234567");
    assert_eq!(outbound.cost_dollars, 12.5);
    assert_eq!(outbound.start_datetime, "2026-07-02T06:00:00Z");
    assert_eq!(outbound.end_datetime, "2026-07-02T06:01:23Z");
    assert_eq!(outbound.call_id, "call-abc-001");

    // Inbound: the "from" number is the call's number.
    let mut inbound_call = analyzed_call().call;
    inbound_call.direction = "inbound".to_string();
    let inbound = normalize(&inbound_call);
    assert_eq!(inbound.phone_number, "+15550000000");
}

#[tokio::test]
async fn normalize_call_filters_non_analyzed_events() {
    let mut other = analyzed_call();
    other.event = "call_started".to_string();
    let err = normalize_call(other).await.expect_err("must be filtered");
    assert!(err.to_string().contains("call_started"), "got: {err}");

    let record = normalize_call(analyzed_call())
        .await
        .expect("analyzed event passes the filter");
    assert_eq!(record.call_id, "call-abc-001");
}

#[test]
fn record_columns_and_notion_properties_are_pure_projections() {
    let record = normalize(&analyzed_call().call);
    let columns = record_columns(&record);
    assert_eq!(columns["Call ID"], serde_json::json!("call-abc-001"));
    assert_eq!(columns["Total Cost in Dollars"], serde_json::json!(12.5));

    let props = notion_properties(&record);
    assert_eq!(
        props["Call Summary"]["title"][0]["text"]["content"],
        serde_json::json!("Caller booked a standard room.")
    );
    assert_eq!(
        props["Phone Number"]["phone_number"],
        serde_json::json!("+15551234567")
    );
    assert_eq!(
        props["Start Datetime"]["date"]["start"],
        serde_json::json!("2026-07-02T06:00:00Z")
    );
}

// ---- Flow validation + requirements ----------------------------------------

#[test]
fn flow_validates_and_derives_http_trigger_requirement() {
    let ir = validated_ir();
    let requirements = derive_requirements(&ir);

    let trigger = requirements
        .triggers
        .iter()
        .find(|t| t.alias == TRIGGER_ALIAS)
        .expect("intake trigger requirement present");
    assert_eq!(trigger.kind, TriggerKind::Http);

    let entrypoint = requirements
        .entrypoints
        .iter()
        .find(|e| e.trigger_alias == TRIGGER_ALIAS)
        .expect("http entrypoint requirement present");
    assert_eq!(entrypoint.capture_alias, "capture");
    assert_eq!(entrypoint.method.as_deref(), Some("POST"));
    assert_eq!(entrypoint.route_path.as_deref(), Some("/retell"));
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

    // Airtable create: http_write only.
    assert!(hints("store_in_airtable").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("store_in_airtable").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Sheets append also reads the header row (http_read + http_write).
    assert!(hints("store_in_sheets").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(hints("store_in_sheets").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Notion create: http_write only.
    assert!(hints("store_in_notion").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("store_in_notion").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Terminal KV upsert.
    assert!(hints("record_sink").contains(&EffectHint::KvWrite.as_str().to_string()));

    // Every connector node carries its connector-prefixed op identifier.
    let identifier = |alias: &str| {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .map(|n| n.identifier.clone())
            .expect("node present")
    };
    assert_eq!(
        identifier("store_in_airtable"),
        "connector.airtable.store_transcript"
    );
    assert_eq!(
        identifier("store_in_sheets"),
        "connector.google.sheets.append_transcript_row"
    );
    assert_eq!(
        identifier("store_in_notion"),
        "connector.notion.store_transcript_page"
    );
}

// ---- record_sink honesty + idempotency (s15/s16 pattern) --------------------

fn kv_bag() -> (Arc<MemoryKv>, Arc<dyn ResourceAccess>) {
    let kv = Arc::new(MemoryKv::new());
    let bag: Arc<dyn ResourceAccess> = Arc::new(ResourceBag::default().with_kv(kv.clone()));
    (kv, bag)
}

fn sample_notion_stored(call_id: &str) -> NotionStored {
    let mut call = analyzed_call().call;
    call.call_id = call_id.to_string();
    NotionStored {
        record: normalize(&call),
        airtable_record_id: "rec-1".to_string(),
        sheet_updated_range: "'Transcripts'!A2:I2".to_string(),
        notion_page_id: "page-1".to_string(),
    }
}

#[tokio::test]
async fn record_sink_succeeds_under_exactly_declared_kv_hints() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_sink",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let record = context::with_resources(view, async {
        record_sink(sample_notion_stored("call-abc-001"))
            .await
            .expect("write succeeds under declared hints")
    })
    .await;

    assert!(record.stored);
    assert!(
        scoped.take_denials().is_empty(),
        "declared hints must be sufficient: no CAP110 denials"
    );
}

#[tokio::test]
async fn record_sink_denied_without_kv_grant() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new("record_sink", bag, []));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        record_sink(sample_notion_stored("call-abc-001"))
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
async fn record_sink_is_idempotent_on_call_id() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_sink",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));

    let first = context::with_resources(scoped.clone(), async {
        record_sink(sample_notion_stored("call-abc-001"))
            .await
            .expect("first")
    })
    .await;
    let second = context::with_resources(scoped.clone(), async {
        record_sink(sample_notion_stored("call-abc-001"))
            .await
            .expect("redelivery")
    })
    .await;

    assert!(first.stored, "first delivery writes the record");
    assert!(!second.stored, "redelivery is deduplicated");
    assert_eq!(first.key, second.key);

    let other = context::with_resources(scoped.clone(), async {
        record_sink(sample_notion_stored("call-xyz-999"))
            .await
            .expect("a distinct call")
    })
    .await;
    assert!(other.stored, "a distinct call writes its own record");
    assert_ne!(first.key, other.key);
}

// ---- Compose: webhook -> airtable -> sheets -> notion -> record -------------

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn webhook_fans_call_into_three_sinks_and_records_idempotent_summary() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _airtable = EnvGuard::set(AIRTABLE_ENDPOINT_ENV, &server.base_url());
    let _sheets = EnvGuard::set(SHEETS_ENDPOINT_ENV, &server.base_url());
    let _notion = EnvGuard::set(NOTION_ENDPOINT_ENV, &server.base_url());
    let _airtable_auth = EnvGuard::set(AIRTABLE_AUTH_ENV, "s18-airtable-token");
    let _google_auth = EnvGuard::set(GOOGLE_AUTH_ENV, "s18-google-token");
    let _notion_auth = EnvGuard::set(NOTION_AUTH_ENV, "s18-notion-token");

    // 1. Airtable record.create.
    let airtable_create = server.mock(|when, then| {
        when.method(POST)
            .path("/v0/appLatticePilot01/Transcripts")
            .header("authorization", "Bearer s18-airtable-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "id": "rec-air-1",
            "createdTime": "2026-07-02T06:02:00.000Z"
        }));
    });
    // 2. Sheets append reads the header row then appends the ordered row.
    let values_read = server.mock(|when, then| {
        when.method(GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [[
                "Call ID", "Start Datetime", "End Datetime", "Duration in seconds",
                "Phone Number", "Transcript", "Call Summary", "User Sentiment",
                "Total Cost in Dollars"
            ]]
        }));
    });
    let values_append = server.mock(|when, then| {
        when.method(POST).path_contains("append");
        then.status(200).json_body_obj(&serde_json::json!({
            "updates": { "updatedRange": "'Transcripts'!A2:I2" }
        }));
    });
    // 3. Notion databasePage create.
    let notion_create = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/pages")
            .header("authorization", "Bearer s18-notion-token")
            .header(
                "notion-version",
                connector_notion::runtime::notion_api::NOTION_VERSION,
            );
        then.status(200).json_body_obj(&serde_json::json!({
            "object": "page",
            "id": "page-notion-1",
            "url": "https://www.notion.so/page-notion-1"
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

    let fire = || Invocation::new(TRIGGER_ALIAS, "capture", call_event_json(&analyzed_call()));

    // First delivery.
    let first = runtime.execute(fire()).await.expect("first delivery runs");
    let first = match first {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<SinkRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the first delivery"),
    };
    assert!(first.stored);
    assert_eq!(first.call_id, "call-abc-001");
    assert_eq!(first.airtable_record_id, "rec-air-1");
    assert_eq!(first.sheet_updated_range, "'Transcripts'!A2:I2");
    assert_eq!(first.notion_page_id, "page-notion-1");
    assert_eq!(first.key, sink_key("call-abc-001"));

    airtable_create.assert_hits(1);
    values_read.assert_hits(1);
    values_append.assert_hits(1);
    notion_create.assert_hits(1);

    // Redelivery of the SAME call: the three writes replay byte-identical and
    // the terminal KV record dedupes.
    let second = runtime.execute(fire()).await.expect("redelivery runs");
    let second = match second {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<SinkRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the redelivery"),
    };
    assert!(
        !second.stored,
        "redelivery must not write a duplicate record"
    );
    assert_eq!(second.key, first.key);

    // Exactly one durable record for the call.
    let raw = kv
        .get(&sink_key("call-abc-001"))
        .await
        .expect("kv get")
        .expect("record present");
    let row: SinkRecord = serde_json::from_slice(&raw).expect("decode row");
    assert_eq!(row.notion_page_id, "page-notion-1");
}
