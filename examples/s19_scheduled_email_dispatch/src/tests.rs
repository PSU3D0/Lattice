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
use connector_google_sheets::runtime::transport::EnvConnectorRuntime;
use dag_core::EffectHint;
use dag_core::requirements::TriggerKind;
use host_inproc::{HostExecutionResult, HostRuntime, Invocation};
use httpmock::Method::{GET, POST, PUT};
use httpmock::MockServer;
use kernel_plan::derive_requirements;

/// 2026-07-02T08:00:00Z — one daily fire.
const FIRE_MS: u64 = 1_782_979_200_000;

// ---- In-memory checkpoint store (same shape as s15/s16's test double) -----

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
const GMAIL_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_GMAIL_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";

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

fn row(fields: &[(&str, &str)]) -> GoogleSheetsRowMatch {
    let map: serde_json::Map<String, JsonValue> = fields
        .iter()
        .map(|(k, v)| ((*k).to_string(), json!(v)))
        .collect();
    GoogleSheetsRowMatch {
        row_number: 2,
        values: JsonValue::Object(map),
    }
}

fn scheduled_event(scheduled_time_ms: u64) -> serde_json::Value {
    serde_json::to_value(ScheduledEvent {
        scheduled_time_ms,
        cron: SCHEDULE_CRON.to_string(),
    })
    .expect("serialize scheduled event")
}

/// The queue table the mock returns: one due row (m1, past date), one future
/// row (m2). The already-sent m3 is not queued, so `find_rows` never returns
/// it; the mock omits it to keep the fixture focused on the filter under test.
fn queue_values() -> serde_json::Value {
    json!({
        "values": [
            ["id", "email", "name", "subject", "body", "send_date", "status"],
            ["m1", "alice@lattice-pilot.test", "Alice", "Welcome",
             "Your onboarding is ready.", "2020-01-01", "queued"],
            ["m2", "bob@lattice-pilot.test", "Bob", "Reminder",
             "Your renewal is coming up.", "2999-01-01", "queued"],
        ]
    })
}

// ---- Pure helpers ----------------------------------------------------------

#[test]
fn utc_date_handles_epoch_and_leap_years() {
    assert_eq!(utc_date(0), "1970-01-01");
    assert_eq!(utc_date(86_400_000), "1970-01-02");
    assert_eq!(utc_date(1_709_164_800_000), "2024-02-29");
    assert_eq!(utc_date(FIRE_MS), "2026-07-02");
}

#[tokio::test]
async fn plan_dispatch_is_a_pure_function_of_the_fire() {
    let plan = plan_dispatch(ScheduledEvent {
        scheduled_time_ms: FIRE_MS,
        cron: SCHEDULE_CRON.to_string(),
    })
    .await
    .expect("plan");

    assert_eq!(plan.date, "2026-07-02");
    assert_eq!(plan.scheduled_time_ms, FIRE_MS);
    assert_eq!(
        dispatch_key(FIRE_MS),
        format!("{FLOW_NAME}:{TRIGGER_ALIAS}:{FIRE_MS}")
    );
}

#[test]
fn due_message_gates_on_arrival_and_required_fields() {
    let date = "2026-07-02";

    // Due: send_date in the past, all fields present.
    let ready = row(&[
        ("id", "m1"),
        ("email", "alice@lattice-pilot.test"),
        ("name", "Alice"),
        ("subject", "Welcome"),
        ("body", "Your onboarding is ready."),
        ("send_date", "2020-01-01"),
        ("status", "queued"),
    ]);
    let due = due_message(&ready, date).expect("row is due");
    assert_eq!(due.id, "m1");
    assert_eq!(due.email, "alice@lattice-pilot.test");

    // Not yet due: send_date in the future.
    let future = row(&[
        ("id", "m2"),
        ("email", "bob@lattice-pilot.test"),
        ("subject", "Reminder"),
        ("body", "Later."),
        ("send_date", "2999-01-01"),
    ]);
    assert!(due_message(&future, date).is_none());

    // Incomplete: missing email.
    let incomplete = row(&[
        ("id", "m4"),
        ("subject", "Hi"),
        ("body", "No address."),
        ("send_date", "2020-01-01"),
    ]);
    assert!(due_message(&incomplete, date).is_none());

    // On the boundary (send_date == today) is due.
    let today = row(&[
        ("id", "m5"),
        ("email", "carol@lattice-pilot.test"),
        ("subject", "Now"),
        ("body", "Today."),
        ("send_date", date),
    ]);
    assert!(due_message(&today, date).is_some());
}

#[test]
fn message_text_personalizes_by_name() {
    let named = DueMessage {
        id: "m1".into(),
        email: "a@lattice-pilot.test".into(),
        name: "Alice".into(),
        subject: "Welcome".into(),
        body: "Your onboarding is ready.".into(),
        send_date: "2020-01-01".into(),
    };
    let text = message_text(&named);
    assert!(text.contains("Hi Alice,"));
    assert!(text.contains("Your onboarding is ready."));

    let anon = DueMessage {
        name: String::new(),
        ..named
    };
    assert!(message_text(&anon).contains("Hello,"));
}

// ---- Flow validation + requirements ----------------------------------------

#[test]
fn flow_validates_and_derives_schedule_requirement() {
    let ir = validated_ir();
    let requirements = derive_requirements(&ir);

    let trigger = requirements
        .triggers
        .iter()
        .find(|t| t.alias == TRIGGER_ALIAS)
        .expect("dispatch_trigger requirement present");
    assert_eq!(trigger.kind, TriggerKind::Schedule);
    assert_eq!(trigger.crons, vec![SCHEDULE_CRON.to_string()]);

    let entrypoint = requirements
        .entrypoints
        .iter()
        .find(|e| e.trigger_alias == TRIGGER_ALIAS)
        .expect("schedule entrypoint requirement present");
    assert_eq!(entrypoint.schedule.as_deref(), Some(SCHEDULE_CRON));
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

    // Read-only sheet query: http_read only.
    assert!(hints("load_queue").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(!hints("load_queue").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // Gmail send: http_write only (this flow reads nothing from Gmail).
    assert!(hints("dispatch_messages").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("dispatch_messages").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Upsert reads the table then writes the matched row: http_read + http_write.
    assert!(hints("mark_messages_sent").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(hints("mark_messages_sent").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Terminal KV upsert.
    assert!(hints("record_dispatch").contains(&EffectHint::KvWrite.as_str().to_string()));

    // Every connector node carries its connector-prefixed op identifier.
    let identifier = |alias: &str| {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .map(|n| n.identifier.clone())
            .expect("node present")
    };
    assert_eq!(
        identifier("load_queue"),
        "connector.google.sheets.load_message_queue"
    );
    assert_eq!(
        identifier("dispatch_messages"),
        "connector.google.gmail.dispatch_messages"
    );
    assert_eq!(
        identifier("mark_messages_sent"),
        "connector.google.sheets.mark_messages_sent"
    );
}

// ---- record_dispatch honesty + idempotency (s15/s16 pattern) ----------------

fn kv_bag() -> (Arc<MemoryKv>, Arc<dyn ResourceAccess>) {
    let kv = Arc::new(MemoryKv::new());
    let bag: Arc<dyn ResourceAccess> = Arc::new(ResourceBag::default().with_kv(kv.clone()));
    (kv, bag)
}

fn sample_batch(scheduled_time_ms: u64) -> MarkedBatch {
    MarkedBatch {
        plan: DispatchPlan {
            scheduled_time_ms,
            cron: SCHEDULE_CRON.to_string(),
            date: utc_date(scheduled_time_ms),
        },
        message_ids: vec!["msg-1".to_string()],
        sent_count: 1,
        marked_count: 1,
    }
}

#[tokio::test]
async fn record_dispatch_succeeds_under_exactly_declared_kv_hints() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_dispatch",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let record = context::with_resources(view, async {
        record_dispatch(sample_batch(FIRE_MS))
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
async fn record_dispatch_denied_without_kv_grant() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new("record_dispatch", bag, []));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        record_dispatch(sample_batch(FIRE_MS))
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
async fn record_dispatch_is_idempotent_on_scheduled_time() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_dispatch",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));

    let first = context::with_resources(scoped.clone(), async {
        record_dispatch(sample_batch(FIRE_MS)).await.expect("first")
    })
    .await;
    let second = context::with_resources(scoped.clone(), async {
        record_dispatch(sample_batch(FIRE_MS))
            .await
            .expect("redelivery")
    })
    .await;

    assert!(first.stored, "first delivery writes the record");
    assert!(!second.stored, "redelivery is deduplicated");
    assert_eq!(first.key, second.key);

    let other = context::with_resources(scoped.clone(), async {
        record_dispatch(sample_batch(FIRE_MS + 86_400_000))
            .await
            .expect("next day's fire")
    })
    .await;
    assert!(other.stored, "a distinct fire writes its own record");
    assert_ne!(first.key, other.key);
}

// ---- Compose: fire -> queue read -> select -> send -> mark -> record --------

// The env lock is held across awaits deliberately: the process-global
// endpoint/auth env vars must not be mutated by a parallel test mid-fire.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn scheduled_fire_dispatches_due_messages_and_records_idempotent_summary() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _sheets = EnvGuard::set(SHEETS_ENDPOINT_ENV, &server.base_url());
    let _gmail = EnvGuard::set(GMAIL_ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "s19-test-token");

    // 1. find_rows + upsert_row both read the queue table via GET /values/.
    let values_read = server.mock(|when, then| {
        when.method(GET)
            .path_contains("/values/")
            .header("authorization", "Bearer s19-test-token");
        then.status(200).json_body_obj(&queue_values());
    });
    // 2. gmail send for the single due message (m1).
    let gmail_send = server.mock(|when, then| {
        when.method(POST)
            .path("/gmail/v1/users/me/messages/send")
            .header("authorization", "Bearer s19-test-token");
        then.status(200).json_body_obj(&json!({
            "id": "msg-dispatch-1", "threadId": "thread-1", "labelIds": ["SENT"]
        }));
    });
    // 3. upsert_row updates the matched row via PUT /values/.
    let values_update = server.mock(|when, then| {
        when.method(PUT).path_contains("/values/");
        then.status(200).json_body_obj(&json!({
            "updatedRange": "'Queue'!A2:G2"
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

    let fire = || Invocation::new(TRIGGER_ALIAS, "capture", scheduled_event(FIRE_MS));

    // First delivery of the fire.
    let first = runtime.execute(fire()).await.expect("first fire runs");
    let first = match first {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<DispatchRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the first fire"),
    };
    assert!(first.stored);
    assert_eq!(first.sent_count, 1, "only the due row (m1) is sent");
    assert_eq!(first.marked_count, 1);
    assert_eq!(first.message_ids, vec!["msg-dispatch-1".to_string()]);
    assert_eq!(first.key, dispatch_key(FIRE_MS));

    // find_rows read the table once; upsert read it once more before its write.
    values_read.assert_hits(2);
    gmail_send.assert_hits(1);
    values_update.assert_hits(1);

    // Redelivery of the SAME fire: payloads are byte-identical (pure functions
    // of scheduled_time_ms + sheet contents) and the terminal KV record dedupes.
    let second = runtime.execute(fire()).await.expect("redelivery runs");
    let second = match second {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<DispatchRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the redelivery"),
    };
    assert!(
        !second.stored,
        "redelivery must not write a duplicate record"
    );
    assert_eq!(second.key, first.key);

    // Exactly one durable record for the fire.
    let raw = kv
        .get(&dispatch_key(FIRE_MS))
        .await
        .expect("kv get")
        .expect("record present");
    let stored: DispatchRecord = serde_json::from_slice(&raw).expect("decode row");
    assert_eq!(stored.sent_count, 1);
}
