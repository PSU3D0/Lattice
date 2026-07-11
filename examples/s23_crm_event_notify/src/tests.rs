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
use connector_slack_core::runtime::transport::EnvConnectorRuntime;
use dag_core::EffectHint;
use dag_core::requirements::TriggerKind;
use host_inproc::{HostExecutionResult, HostRuntime, Invocation};
use httpmock::Method::{GET, POST};
use httpmock::MockServer;
use kernel_plan::derive_requirements;

// ---- In-memory checkpoint store (same shape as s18's test double) ----------

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

const SHEETS_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_SHEETS_DEFAULT_BASE_URL";
const GMAIL_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_GMAIL_DEFAULT_BASE_URL";
const SLACK_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_SLACK_DEFAULT_BASE_URL";
const GOOGLE_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";
const SLACK_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_SLACK_AUTH";

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

fn create_event() -> CrmEvent {
    CrmEvent {
        event_name: "company.created".to_string(),
        object_metadata: ObjectMetadata {
            id: "evt-create-001".to_string(),
            name_singular: "company".to_string(),
        },
        record: RecordRef {
            id: "rec-comp-42".to_string(),
            type_name: "Company".to_string(),
        },
    }
}

fn delete_event() -> CrmEvent {
    CrmEvent {
        event_name: "person.deleted".to_string(),
        object_metadata: ObjectMetadata {
            id: "evt-delete-001".to_string(),
            name_singular: "person".to_string(),
        },
        record: RecordRef {
            id: "rec-pers-7".to_string(),
            type_name: "Person".to_string(),
        },
    }
}

fn event_json(event: &CrmEvent) -> serde_json::Value {
    serde_json::to_value(event).expect("serialize crm event")
}

// ---- Pure helpers ----------------------------------------------------------

#[test]
fn action_and_channel_routing() {
    assert_eq!(action_of("person.deleted"), "deleted");
    assert_eq!(action_of("company.created"), "created");
    assert_eq!(action_of("noseparator"), "");

    // The template routes the literal `delete` segment to email; the corpus
    // splits on `.` and compares to "delete".
    assert_eq!(channel_for("delete"), EMAIL_CHANNEL);
    assert_eq!(channel_for("created"), MESSAGE_CHANNEL);
    assert_eq!(channel_for("updated"), MESSAGE_CHANNEL);
}

#[test]
fn normalize_projects_fields_and_routes_channel() {
    let created = normalize(&create_event());
    assert_eq!(created.action, "created");
    assert_eq!(created.channel, MESSAGE_CHANNEL);
    assert_eq!(created.object_id, "evt-create-001");
    assert_eq!(created.record_id, "rec-comp-42");
    assert_eq!(created.record_type, "Company");

    let deleted = normalize(&delete_event());
    assert_eq!(deleted.action, "deleted");
    // `deleted`.split gives `deleted` != `delete`; this template's `.split(".")[1]`
    // compares to the literal `delete`, so only an action segment of exactly
    // `delete` routes to email. Model a true delete action explicitly:
    let mut hard_delete = delete_event();
    hard_delete.event_name = "person.delete".to_string();
    assert_eq!(normalize(&hard_delete).channel, EMAIL_CHANNEL);
}

#[tokio::test]
async fn normalize_event_requires_event_name() {
    let mut blank = create_event();
    blank.event_name = "   ".to_string();
    let err = normalize_event(blank).await.expect_err("must be filtered");
    assert!(err.to_string().contains("event_name"), "got: {err}");

    let ok = normalize_event(create_event())
        .await
        .expect("valid event passes the filter");
    assert_eq!(ok.object_id, "evt-create-001");
}

#[test]
fn event_columns_is_a_pure_projection() {
    let columns = event_columns(&normalize(&delete_event()));
    assert_eq!(columns["event_name"], serde_json::json!("person.deleted"));
    assert_eq!(columns["object_id"], serde_json::json!("evt-delete-001"));
    assert_eq!(columns["record_id"], serde_json::json!("rec-pers-7"));
    assert_eq!(columns["channel"], serde_json::json!("message"));
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
        .expect("crm_event trigger requirement present");
    assert_eq!(trigger.kind, TriggerKind::Http);

    let entrypoint = requirements
        .entrypoints
        .iter()
        .find(|e| e.trigger_alias == TRIGGER_ALIAS)
        .expect("http entrypoint requirement present");
    assert_eq!(entrypoint.capture_alias, "capture");
    assert_eq!(entrypoint.method.as_deref(), Some("POST"));
    assert_eq!(entrypoint.route_path.as_deref(), Some("/crm-event"));
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

    // Sheets append reads the header row then writes (http_read + http_write).
    assert!(hints("log_event").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(hints("log_event").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // Gmail send: http_write only.
    assert!(hints("notify_email").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("notify_email").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Slack post: http_write only.
    assert!(hints("notify_slack").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("notify_slack").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Terminal KV upsert.
    assert!(hints("record_delivery").contains(&EffectHint::KvWrite.as_str().to_string()));

    // Every connector node carries its connector-prefixed op identifier.
    let identifier = |alias: &str| {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .map(|n| n.identifier.clone())
            .expect("node present")
    };
    assert_eq!(identifier("log_event"), "connector.google.sheets.log_event");
    assert_eq!(
        identifier("notify_email"),
        "connector.google.gmail.notify_email"
    );
    assert_eq!(
        identifier("notify_slack"),
        "connector.slack.core.notify_slack"
    );
}

// ---- record_delivery honesty + idempotency (s15/s16/s18 pattern) ------------

fn kv_bag() -> (Arc<MemoryKv>, Arc<dyn ResourceAccess>) {
    let kv = Arc::new(MemoryKv::new());
    let bag: Arc<dyn ResourceAccess> = Arc::new(ResourceBag::default().with_kv(kv.clone()));
    (kv, bag)
}

fn sample_notification(object_id: &str) -> EventNotification {
    EventNotification {
        channel: MESSAGE_CHANNEL.to_string(),
        event_name: "company.created".to_string(),
        object_id: object_id.to_string(),
        record_id: "rec-comp-42".to_string(),
        logged_range: "'Events'!A2:G2".to_string(),
        delivery_ref: "1700000000.000200".to_string(),
    }
}

#[tokio::test]
async fn record_delivery_succeeds_under_exactly_declared_kv_hints() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_delivery",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let record = context::with_resources(view, async {
        record_delivery(sample_notification("evt-create-001"))
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
async fn record_delivery_denied_without_kv_grant() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new("record_delivery", bag, []));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        record_delivery(sample_notification("evt-create-001"))
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
async fn record_delivery_is_idempotent_on_event_id() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_delivery",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));

    let first = context::with_resources(scoped.clone(), async {
        record_delivery(sample_notification("evt-create-001"))
            .await
            .expect("first")
    })
    .await;
    let second = context::with_resources(scoped.clone(), async {
        record_delivery(sample_notification("evt-create-001"))
            .await
            .expect("redelivery")
    })
    .await;

    assert!(first.stored, "first delivery writes the record");
    assert!(!second.stored, "redelivery is deduplicated");
    assert_eq!(first.key, second.key);

    let other = context::with_resources(scoped.clone(), async {
        record_delivery(sample_notification("evt-delete-999"))
            .await
            .expect("a distinct event")
    })
    .await;
    assert!(other.stored, "a distinct event writes its own record");
    assert_ne!(first.key, other.key);
}

// ---- Compose: webhook -> log -> switch(email|slack) -> record ---------------

struct Mocks<'a> {
    values_read: httpmock::Mock<'a>,
    values_append: httpmock::Mock<'a>,
    gmail_send: httpmock::Mock<'a>,
    slack_post: httpmock::Mock<'a>,
}

fn mount_mocks(server: &MockServer) -> Mocks<'_> {
    let values_read = server.mock(|when, then| {
        when.method(GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [[
                "event_name", "action", "object_id", "object_name",
                "record_id", "record_type", "channel"
            ]]
        }));
    });
    let values_append = server.mock(|when, then| {
        when.method(POST).path_contains("append");
        then.status(200).json_body_obj(&serde_json::json!({
            "updates": { "updatedRange": "'Events'!A2:G2" }
        }));
    });
    let gmail_send = server.mock(|when, then| {
        when.method(POST)
            .path_contains("messages/send")
            .header("authorization", "Bearer s23-google-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "id": "gmail-msg-1",
            "threadId": "gmail-thread-1",
            "labelIds": ["SENT"]
        }));
    });
    let slack_post = server.mock(|when, then| {
        when.method(POST)
            .path("/chat.postMessage")
            .header("authorization", "Bearer s23-slack-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": true, "channel": "C-crm-events", "ts": "1700000000.000200"
        }));
    });
    Mocks {
        values_read,
        values_append,
        gmail_send,
        slack_post,
    }
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn webhook_routes_delete_to_email_and_other_to_slack_idempotently() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _sheets = EnvGuard::set(SHEETS_ENDPOINT_ENV, &server.base_url());
    let _gmail = EnvGuard::set(GMAIL_ENDPOINT_ENV, &server.base_url());
    let _slack = EnvGuard::set(SLACK_ENDPOINT_ENV, &server.base_url());
    let _google_auth = EnvGuard::set(GOOGLE_AUTH_ENV, "s23-google-token");
    let _slack_auth = EnvGuard::set(SLACK_AUTH_ENV, "s23-slack-token");

    let mocks = mount_mocks(&server);

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

    // A `person.delete` event routes to the email channel.
    let mut hard_delete = delete_event();
    hard_delete.event_name = "person.delete".to_string();
    let delete_fire = || Invocation::new(TRIGGER_ALIAS, "capture", event_json(&hard_delete));

    let out = runtime.execute(delete_fire()).await.expect("delete runs");
    let delete_record = match out {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<EventDeliveryRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the delete delivery"),
    };
    assert!(delete_record.stored);
    assert_eq!(delete_record.channel, EMAIL_CHANNEL);
    assert_eq!(delete_record.delivery_ref, "gmail-msg-1");
    assert_eq!(delete_record.key, delivery_key("evt-delete-001"));

    mocks.values_read.assert_hits(1);
    mocks.values_append.assert_hits(1);
    mocks.gmail_send.assert_hits(1);
    mocks.slack_post.assert_hits(0);

    // A `company.created` event routes to the Slack channel.
    let create_fire = || Invocation::new(TRIGGER_ALIAS, "capture", event_json(&create_event()));
    let out = runtime.execute(create_fire()).await.expect("create runs");
    let create_record = match out {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<EventDeliveryRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the create delivery"),
    };
    assert!(create_record.stored);
    assert_eq!(create_record.channel, MESSAGE_CHANNEL);
    assert_eq!(create_record.delivery_ref, "1700000000.000200");
    assert_eq!(create_record.key, delivery_key("evt-create-001"));

    mocks.gmail_send.assert_hits(1); // unchanged: create did not email
    mocks.slack_post.assert_hits(1);
    mocks.values_append.assert_hits(2); // both events logged

    // Redelivery of the delete event: byte-identical replay, KV dedupes.
    let second = runtime
        .execute(delete_fire())
        .await
        .expect("redelivery runs");
    let second = match second {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<EventDeliveryRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the redelivery"),
    };
    assert!(
        !second.stored,
        "redelivery must not write a duplicate record"
    );
    assert_eq!(second.key, delete_record.key);

    // Exactly one durable record for the delete event.
    let raw = kv
        .get(&delivery_key("evt-delete-001"))
        .await
        .expect("kv get")
        .expect("record present");
    let row: EventDeliveryRecord = serde_json::from_slice(&raw).expect("decode row");
    assert_eq!(row.delivery_ref, "gmail-msg-1");
}
