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
use connector_hunter::runtime::transport::EnvConnectorRuntime;
use dag_core::EffectHint;
use dag_core::requirements::TriggerKind;
use host_inproc::{HostExecutionResult, HostRuntime, Invocation};
use httpmock::Method::{GET, POST, PUT};
use httpmock::MockServer;
use kernel_plan::derive_requirements;

// ---- In-memory checkpoint store (same shape as s16/s18's test double) ------

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

const HUNTER_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_HUNTER_DEFAULT_BASE_URL";
const SHEETS_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_SHEETS_DEFAULT_BASE_URL";
const GMAIL_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_GMAIL_DEFAULT_BASE_URL";
const DISCORD_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_DISCORD_WEBHOOK_DEFAULT_BASE_URL";
const HUNTER_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_HUNTER_API_KEY_AUTH";
const GOOGLE_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";
const DISCORD_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_DISCORD_WEBHOOK_AUTH";

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

fn lead() -> LeadSubmission {
    LeadSubmission {
        name: "Ada Lovelace".to_string(),
        email: "ada@leads.test".to_string(),
        query: "Do you support cron triggers?".to_string(),
        submitted_at: "2026-07-11T09:00:00Z".to_string(),
    }
}

fn lead_json(submission: &LeadSubmission) -> serde_json::Value {
    serde_json::to_value(submission).expect("serialize lead")
}

// ---- Pure helpers ----------------------------------------------------------

#[test]
fn notification_body_and_subject_are_pure_functions_of_the_lead() {
    let body = notification_body(&lead());
    assert!(body.contains("Ada Lovelace"));
    assert!(body.contains("ada@leads.test"));
    assert!(body.contains("Do you support cron triggers?"));
    assert_eq!(notification_subject(&lead()), "New lead from Ada Lovelace");
}

#[test]
fn deliverability_gate_only_accepts_deliverable() {
    assert!(is_deliverable("deliverable"));
    assert!(is_deliverable("DELIVERABLE"));
    assert!(!is_deliverable("undeliverable"));
    assert!(!is_deliverable("risky"));
    assert!(!is_deliverable("unknown"));
}

#[tokio::test]
async fn guard_halts_a_non_deliverable_lead() {
    let verified = VerifiedLead {
        submission: lead(),
        status: "invalid".to_string(),
        result: "undeliverable".to_string(),
        score: 0,
    };
    let err = guard_deliverable(verified)
        .await
        .expect_err("a fake email must halt the flow");
    assert!(err.to_string().contains("undeliverable"), "got: {err}");

    let ok = VerifiedLead {
        submission: lead(),
        status: "valid".to_string(),
        result: "deliverable".to_string(),
        score: 92,
    };
    let passed = guard_deliverable(ok)
        .await
        .expect("deliverable lead passes");
    assert_eq!(passed.result, "deliverable");
}

// ---- Flow validation + requirements ----------------------------------------

#[test]
fn flow_validates_and_derives_webhook_requirement() {
    let ir = validated_ir();
    let requirements = derive_requirements(&ir);

    let trigger = requirements
        .triggers
        .iter()
        .find(|t| t.alias == TRIGGER_ALIAS)
        .expect("lead_trigger requirement present");
    assert_eq!(trigger.kind, TriggerKind::Http);

    let entrypoint = requirements
        .entrypoints
        .iter()
        .find(|e| e.trigger_alias == TRIGGER_ALIAS)
        .expect("http entrypoint requirement present");
    assert_eq!(entrypoint.capture_alias, "capture");
    assert_eq!(entrypoint.method.as_deref(), Some("POST"));
    assert_eq!(entrypoint.route_path.as_deref(), Some("/lead-intake"));
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

    // Hunter verify: http_read only.
    assert!(hints("verify_lead").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(!hints("verify_lead").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // Sheets upsert reads the header row then writes: http_read + http_write.
    assert!(hints("record_lead_in_sheet").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(hints("record_lead_in_sheet").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // Gmail send: http_write only.
    assert!(hints("notify_lead_by_email").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("notify_lead_by_email").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Discord post: http_write only.
    assert!(
        hints("announce_lead_on_discord").contains(&EffectHint::HttpWrite.as_str().to_string())
    );
    assert!(
        !hints("announce_lead_on_discord").contains(&EffectHint::HttpRead.as_str().to_string())
    );
    // Terminal KV upsert.
    assert!(hints("record_lead").contains(&EffectHint::KvWrite.as_str().to_string()));

    // Every connector node carries its connector-prefixed op identifier.
    let identifier = |alias: &str| {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .map(|n| n.identifier.clone())
            .expect("node present")
    };
    assert_eq!(
        identifier("verify_lead"),
        "connector.hunter.verify_lead_email"
    );
    assert_eq!(
        identifier("record_lead_in_sheet"),
        "connector.google.sheets.record_lead"
    );
    assert_eq!(
        identifier("notify_lead_by_email"),
        "connector.google.gmail.notify_lead"
    );
    assert_eq!(
        identifier("announce_lead_on_discord"),
        "connector.discord.announce_lead"
    );
}

// ---- record_lead honesty + idempotency (s16/s18 record-node pattern) -------

fn kv_bag() -> (Arc<MemoryKv>, Arc<dyn ResourceAccess>) {
    let kv = Arc::new(MemoryKv::new());
    let bag: Arc<dyn ResourceAccess> = Arc::new(ResourceBag::default().with_kv(kv.clone()));
    (kv, bag)
}

fn sample_announced(email: &str) -> AnnouncedLead {
    let mut submission = lead();
    submission.email = email.to_string();
    AnnouncedLead {
        submission,
        updated_range: "'Leads'!A2:D2".to_string(),
        gmail_message_id: "gmail-1".to_string(),
        discord_delivered: true,
    }
}

#[tokio::test]
async fn record_lead_succeeds_under_exactly_declared_kv_hints() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_lead",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let record = context::with_resources(view, async {
        record_lead(sample_announced("ada@leads.test"))
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
async fn record_lead_denied_without_kv_grant() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new("record_lead", bag, []));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        record_lead(sample_announced("ada@leads.test"))
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
async fn record_lead_is_idempotent_on_email() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_lead",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));

    let first = context::with_resources(scoped.clone(), async {
        record_lead(sample_announced("ada@leads.test"))
            .await
            .expect("first")
    })
    .await;
    let second = context::with_resources(scoped.clone(), async {
        record_lead(sample_announced("ada@leads.test"))
            .await
            .expect("redelivery")
    })
    .await;

    assert!(first.stored, "first delivery writes the record");
    assert!(!second.stored, "redelivery is deduplicated");
    assert_eq!(first.key, second.key);

    let other = context::with_resources(scoped.clone(), async {
        record_lead(sample_announced("grace@leads.test"))
            .await
            .expect("a distinct lead")
    })
    .await;
    assert!(other.stored, "a distinct lead writes its own record");
    assert_ne!(first.key, other.key);
}

// ---- Compose: webhook -> verify -> guard -> sheet -> gmail -> discord -> record

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn webhook_verifies_then_fans_lead_into_three_sinks_idempotently() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _hunter = EnvGuard::set(HUNTER_ENDPOINT_ENV, &server.base_url());
    let _sheets = EnvGuard::set(SHEETS_ENDPOINT_ENV, &server.base_url());
    let _gmail = EnvGuard::set(GMAIL_ENDPOINT_ENV, &server.base_url());
    let _discord = EnvGuard::set(DISCORD_ENDPOINT_ENV, &server.base_url());
    let _hunter_auth = EnvGuard::set(HUNTER_AUTH_ENV, "s24-hunter-key");
    let _google_auth = EnvGuard::set(GOOGLE_AUTH_ENV, "s24-google-token");
    let _discord_auth = EnvGuard::set(DISCORD_AUTH_ENV, "111/s24-webhook-token");

    // 1. Hunter verifies the lead as deliverable.
    let hunter_verify = server.mock(|when, then| {
        when.method(GET)
            .path("/v2/email-verifier")
            .query_param("email", "ada@leads.test")
            .query_param("api_key", "s24-hunter-key");
        then.status(200).json_body_obj(&serde_json::json!({
            "data": { "status": "valid", "result": "deliverable", "score": 92, "email": "ada@leads.test" }
        }));
    });
    // 2. Sheets upsert reads the header row, then the matched row is updated (PUT).
    let values_read = server.mock(|when, then| {
        when.method(GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [
                ["Name", "Email", "Query", "Submitted On"],
                ["Ada Lovelace", "ada@leads.test", "", ""]
            ]
        }));
    });
    let values_update = server.mock(|when, then| {
        when.method(PUT).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "updatedRange": "'Leads'!A2:D2"
        }));
    });
    // 3. Gmail send.
    let gmail_send = server.mock(|when, then| {
        when.method(POST)
            .path_contains("/messages/send")
            .header("authorization", "Bearer s24-google-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "id": "gmail-abc-1", "threadId": "thread-1", "labelIds": ["SENT"]
        }));
    });
    // 4. Discord webhook post (204 No Content).
    let discord_post = server.mock(|when, then| {
        when.method(POST).path_contains("/api/webhooks/");
        then.status(204);
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

    let fire = || Invocation::new(TRIGGER_ALIAS, "capture", lead_json(&lead()));

    // First delivery.
    let first = runtime.execute(fire()).await.expect("first delivery runs");
    let first = match first {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<LeadRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the first delivery"),
    };
    assert!(first.stored);
    assert_eq!(first.email, "ada@leads.test");
    assert_eq!(first.updated_range, "'Leads'!A2:D2");
    assert_eq!(first.gmail_message_id, "gmail-abc-1");
    assert!(first.discord_delivered);
    assert_eq!(first.key, lead_key("ada@leads.test"));

    hunter_verify.assert_hits(1);
    values_read.assert_hits(1);
    values_update.assert_hits(1);
    gmail_send.assert_hits(1);
    discord_post.assert_hits(1);

    // Redelivery of the SAME lead: the four writes replay byte-identical and the
    // terminal KV record dedupes.
    let second = runtime.execute(fire()).await.expect("redelivery runs");
    let second = match second {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<LeadRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the redelivery"),
    };
    assert!(
        !second.stored,
        "redelivery must not write a duplicate record"
    );
    assert_eq!(second.key, first.key);

    // Exactly one durable record for the lead.
    let raw = kv
        .get(&lead_key("ada@leads.test"))
        .await
        .expect("kv get")
        .expect("record present");
    let row: LeadRecord = serde_json::from_slice(&raw).expect("decode row");
    assert!(row.discord_delivered);
}

// ---- Compose: a fake email is halted before any sink fires -----------------

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn webhook_halts_a_fake_email_before_any_sink() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _hunter = EnvGuard::set(HUNTER_ENDPOINT_ENV, &server.base_url());
    let _sheets = EnvGuard::set(SHEETS_ENDPOINT_ENV, &server.base_url());
    let _gmail = EnvGuard::set(GMAIL_ENDPOINT_ENV, &server.base_url());
    let _discord = EnvGuard::set(DISCORD_ENDPOINT_ENV, &server.base_url());
    let _hunter_auth = EnvGuard::set(HUNTER_AUTH_ENV, "s24-hunter-key");
    let _google_auth = EnvGuard::set(GOOGLE_AUTH_ENV, "s24-google-token");
    let _discord_auth = EnvGuard::set(DISCORD_AUTH_ENV, "111/s24-webhook-token");

    let hunter_verify = server.mock(|when, then| {
        when.method(GET).path("/v2/email-verifier");
        then.status(200).json_body_obj(&serde_json::json!({
            "data": { "status": "invalid", "result": "undeliverable", "score": 0, "email": "fake@nope.test" }
        }));
    });
    // Any sink call would be a bug; assert zero hits below.
    let any_write = server.mock(|when, then| {
        when.method(POST);
        then.status(200).body("{}");
    });
    let any_put = server.mock(|when, then| {
        when.method(PUT);
        then.status(200).body("{}");
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

    let mut fake = lead();
    fake.email = "fake@nope.test".to_string();
    // The guard returns a NodeError, which aborts the run: `execute` resolves to
    // an Err (the node failure), never a captured value.
    match runtime
        .execute(Invocation::new(TRIGGER_ALIAS, "capture", lead_json(&fake)))
        .await
    {
        Ok(_) => panic!("a fake lead must fail the flow at the guard, not produce a value"),
        Err(err) => assert!(
            err.to_string().contains("undeliverable"),
            "expected a guard halt on the fake lead, got: {err}"
        ),
    }

    hunter_verify.assert_hits(1);
    assert_eq!(any_write.hits(), 0, "no POST sink may fire for a fake lead");
    assert_eq!(any_put.hits(), 0, "no PUT sink may fire for a fake lead");
    assert!(
        kv.get(&lead_key("fake@nope.test"))
            .await
            .expect("kv get")
            .is_none(),
        "no terminal record for a halted lead"
    );
}
