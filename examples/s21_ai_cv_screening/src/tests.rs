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
use connector_google_gmail::runtime::transport::EnvConnectorRuntime;
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

const LLM_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_LLM_DEFAULT_BASE_URL";
const SHEETS_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_SHEETS_DEFAULT_BASE_URL";
const GMAIL_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_GMAIL_DEFAULT_BASE_URL";
const LLM_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_LLM_API_KEY";
const GOOGLE_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";

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

fn application() -> CvApplication {
    CvApplication {
        full_name: "Ada Lovelace".to_string(),
        email: "ada@applicant.test".to_string(),
        expectation: "5000-6000".to_string(),
        linkedin: "https://linkedin.test/in/ada".to_string(),
        cv_filename: "ada_lovelace_cv.pdf".to_string(),
        resume_text: "10 years building analytical engines and Rust services.".to_string(),
    }
}

fn application_json(app: &CvApplication) -> serde_json::Value {
    serde_json::to_value(app).expect("serialize application")
}

fn openai_completion_body(content: &str) -> serde_json::Value {
    serde_json::json!({
        "id": "chatcmpl-s21",
        "object": "chat.completion",
        "created": 1,
        "model": "gemini-1.5-flash",
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": content, "tool_calls": [] },
            "logprobs": null,
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 40,
            "completion_tokens": 20,
            "total_tokens": 60,
            "prompt_tokens_details": { "cached_tokens": 0 }
        }
    })
}

const RATING_TEXT: &str = "Rating: 9/10. Strong match. Recommend an interview.";

// ---- Pure helpers ----------------------------------------------------------

#[test]
fn screening_key_is_scoped_to_flow_trigger_and_email() {
    assert_eq!(
        screening_key("ada@applicant.test"),
        "s21_ai_cv_screening_flow:screening_trigger:ada@applicant.test"
    );
}

#[test]
fn screening_prompt_embeds_role_and_resume_text() {
    let prompt = screening_prompt(&application());
    assert!(prompt.contains(JOB_TITLE), "role present: {prompt}");
    assert!(
        prompt.contains("analytical engines"),
        "resume text present: {prompt}"
    );
}

#[test]
fn emails_are_pure_functions_of_the_submission() {
    let (c_subject, c_body) = confirmation_email(&application());
    assert_eq!(c_subject, "We received your application");
    assert!(
        c_body.contains("Ada Lovelace"),
        "candidate greeted: {c_body}"
    );

    let (hr_subject, hr_body) = hr_email(&application(), RATING_TEXT);
    assert!(
        hr_subject.contains(JOB_TITLE),
        "role in subject: {hr_subject}"
    );
    assert!(
        hr_body.contains("ada@applicant.test"),
        "email present: {hr_body}"
    );
    assert!(hr_body.contains(RATING_TEXT), "rating relayed: {hr_body}");
}

#[test]
fn candidate_row_uses_our_own_column_keys() {
    let row = candidate_row(&application(), RATING_TEXT);
    assert_eq!(row["full_name"], serde_json::json!("Ada Lovelace"));
    assert_eq!(row["email"], serde_json::json!("ada@applicant.test"));
    assert_eq!(row["cv_filename"], serde_json::json!("ada_lovelace_cv.pdf"));
    assert_eq!(row["ai_rating"], serde_json::json!(RATING_TEXT));
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
        .expect("screening trigger requirement present");
    assert_eq!(trigger.kind, TriggerKind::Http);

    let entrypoint = requirements
        .entrypoints
        .iter()
        .find(|e| e.trigger_alias == TRIGGER_ALIAS)
        .expect("http entrypoint requirement present");
    assert_eq!(entrypoint.capture_alias, "capture");
    assert_eq!(entrypoint.method.as_deref(), Some("POST"));
    assert_eq!(entrypoint.route_path.as_deref(), Some("/cv-screening"));
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

    // LLM completion: http_write only (POST rides the write capability).
    assert!(hints("rate_candidate").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("rate_candidate").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Sheets append reads the header row then writes: http_read + http_write.
    assert!(hints("record_candidate").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(hints("record_candidate").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // Both gmail sends: http_write only.
    for alias in ["confirm_candidate", "notify_hr"] {
        assert!(hints(alias).contains(&EffectHint::HttpWrite.as_str().to_string()));
        assert!(!hints(alias).contains(&EffectHint::HttpRead.as_str().to_string()));
    }
    // Terminal KV upsert.
    assert!(hints("record_screening").contains(&EffectHint::KvWrite.as_str().to_string()));

    // Every connector node carries its connector-prefixed op identifier so the
    // runtime can infer the connection scope.
    let identifier = |alias: &str| {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .map(|n| n.identifier.clone())
            .expect("node present")
    };
    assert_eq!(identifier("rate_candidate"), "connector.llm.rate_candidate");
    assert_eq!(
        identifier("record_candidate"),
        "connector.google.sheets.record_candidate"
    );
    assert_eq!(
        identifier("confirm_candidate"),
        "connector.google.gmail.confirm_candidate"
    );
    assert_eq!(identifier("notify_hr"), "connector.google.gmail.notify_hr");
}

// ---- record_screening honesty + idempotency (s18 pattern) ------------------

fn kv_bag() -> (Arc<MemoryKv>, Arc<dyn ResourceAccess>) {
    let kv = Arc::new(MemoryKv::new());
    let bag: Arc<dyn ResourceAccess> = Arc::new(ResourceBag::default().with_kv(kv.clone()));
    (kv, bag)
}

fn sample_notified(email: &str) -> NotifiedScreening {
    let mut app = application();
    app.email = email.to_string();
    NotifiedScreening {
        application: app,
        ai_rating: RATING_TEXT.to_string(),
        appended_range: "'Candidates'!A2:F2".to_string(),
        candidate_message_id: "msg-candidate".to_string(),
        hr_message_id: "msg-hr".to_string(),
    }
}

#[tokio::test]
async fn record_screening_succeeds_under_exactly_declared_kv_hints() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_screening",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let record = context::with_resources(view, async {
        record_screening(sample_notified("ada@applicant.test"))
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
async fn record_screening_denied_without_kv_grant() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new("record_screening", bag, []));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        record_screening(sample_notified("ada@applicant.test"))
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
async fn record_screening_is_idempotent_on_email() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_screening",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));

    let first = context::with_resources(scoped.clone(), async {
        record_screening(sample_notified("ada@applicant.test"))
            .await
            .expect("first")
    })
    .await;
    let second = context::with_resources(scoped.clone(), async {
        record_screening(sample_notified("ada@applicant.test"))
            .await
            .expect("redelivery")
    })
    .await;

    assert!(first.stored, "first delivery writes the record");
    assert!(!second.stored, "redelivery is deduplicated");
    assert_eq!(first.key, second.key);

    let other = context::with_resources(scoped.clone(), async {
        record_screening(sample_notified("grace@applicant.test"))
            .await
            .expect("a distinct applicant")
    })
    .await;
    assert!(other.stored, "a distinct applicant writes its own record");
    assert_ne!(first.key, other.key);
}

// ---- Compose: webhook -> llm -> sheets -> gmail x2 -> record ----------------

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn webhook_screens_candidate_appends_row_emails_both_and_records_idempotent() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _llm = EnvGuard::set(LLM_ENDPOINT_ENV, &server.base_url());
    let _sheets = EnvGuard::set(SHEETS_ENDPOINT_ENV, &server.base_url());
    let _gmail = EnvGuard::set(GMAIL_ENDPOINT_ENV, &server.base_url());
    let _llm_auth = EnvGuard::set(LLM_AUTH_ENV, "s21-llm-key");
    let _google_auth = EnvGuard::set(GOOGLE_AUTH_ENV, "s21-google-token");

    // 1. LLM screening.
    let llm_complete = server.mock(|when, then| {
        when.method(POST)
            .path_contains("/chat/completions")
            .header("authorization", "Bearer s21-llm-key");
        then.status(200)
            .json_body(openai_completion_body(RATING_TEXT));
    });
    // 2. Sheets append reads the header row then appends the ordered row.
    let values_read = server.mock(|when, then| {
        when.method(GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [[
                "full_name", "email", "expectation", "linkedin", "cv_filename", "ai_rating"
            ]]
        }));
    });
    let values_append = server.mock(|when, then| {
        when.method(POST).path_contains("append");
        then.status(200).json_body_obj(&serde_json::json!({
            "updates": { "updatedRange": "'Candidates'!A2:F2" }
        }));
    });
    // 3. Both gmail sends hit the same endpoint.
    let gmail_send = server.mock(|when, then| {
        when.method(POST)
            .path("/gmail/v1/users/me/messages/send")
            .header("authorization", "Bearer s21-google-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "id": "msg-sent", "threadId": "thread-sent", "labelIds": ["SENT"]
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

    let fire = || Invocation::new(TRIGGER_ALIAS, "capture", application_json(&application()));

    // First delivery.
    let first = runtime.execute(fire()).await.expect("first delivery runs");
    let first = match first {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<ScreeningRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the first delivery"),
    };
    assert!(first.stored);
    assert_eq!(first.email, "ada@applicant.test");
    assert_eq!(first.ai_rating, RATING_TEXT);
    assert_eq!(first.appended_range, "'Candidates'!A2:F2");
    assert_eq!(first.candidate_message_id, "msg-sent");
    assert_eq!(first.hr_message_id, "msg-sent");
    assert_eq!(first.key, screening_key("ada@applicant.test"));

    llm_complete.assert_hits(1);
    values_read.assert_hits(1);
    values_append.assert_hits(1);
    gmail_send.assert_hits(2);

    // Redelivery of the SAME application: the writes replay byte-identical and
    // the terminal KV record dedupes.
    let second = runtime.execute(fire()).await.expect("redelivery runs");
    let second = match second {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<ScreeningRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the redelivery"),
    };
    assert!(
        !second.stored,
        "redelivery must not write a duplicate record"
    );
    assert_eq!(second.key, first.key);

    // Exactly one durable record for the application.
    let raw = kv
        .get(&screening_key("ada@applicant.test"))
        .await
        .expect("kv get")
        .expect("record present");
    let row: ScreeningRecord = serde_json::from_slice(&raw).expect("decode row");
    assert_eq!(row.candidate_message_id, "msg-sent");
}
