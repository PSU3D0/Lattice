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
use httpmock::Method::{GET, POST};
use httpmock::MockServer;
use kernel_plan::derive_requirements;

// ---- In-memory checkpoint store (same shape as s17's test double) ----------

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
const LLM_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_LLM_DEFAULT_BASE_URL";
const GOOGLE_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";
const LLM_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_LLM_API_KEY";

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

fn sample_request(run_id: &str) -> FeedbackRequest {
    FeedbackRequest {
        run_id: run_id.to_string(),
        spreadsheet_id: "feedback-doc".to_string(),
        sheet: "Form Responses".to_string(),
        model: "mock-model-1".to_string(),
        questions: vec![
            "What went great?".to_string(),
            "How can we improve?".to_string(),
        ],
        recipient: "organizer@lattice-pilot.test".to_string(),
        subject: "Event feedback summary".to_string(),
    }
}

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

/// The form-response table the sheet mock returns: two responses, a mix of
/// answered and blank cells.
fn feedback_values() -> serde_json::Value {
    json!({
        "values": [
            ["What went great?", "How can we improve?", "score"],
            ["Great speakers", "More breaks", "9"],
            ["Good food", "", "7"],
        ]
    })
}

fn mock_completion_body(text: &str) -> serde_json::Value {
    json!({
        "id": "chatcmpl-s25",
        "object": "chat.completion",
        "created": 1,
        "model": "mock-model-1",
        "system_fingerprint": null,
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": text, "tool_calls": [] },
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

// ---- Pure helpers ----------------------------------------------------------

#[test]
fn feedback_key_is_stable_shape() {
    assert_eq!(
        feedback_key("run-7"),
        format!("{FLOW_NAME}:{TRIGGER_ALIAS}:run-7")
    );
}

#[test]
fn aggregate_answers_collects_per_question_dropping_blanks() {
    let rows = vec![
        row(&[
            ("What went great?", "Great speakers"),
            ("How can we improve?", "More breaks"),
        ]),
        row(&[
            ("What went great?", "Good food"),
            ("How can we improve?", ""),
        ]),
    ];
    let questions = vec![
        "What went great?".to_string(),
        "How can we improve?".to_string(),
    ];
    let aggregated = aggregate_answers(&rows, &questions);

    assert_eq!(aggregated.len(), 2);
    assert_eq!(aggregated[0].question, "What went great?");
    assert_eq!(aggregated[0].answers, vec!["Great speakers", "Good food"]);
    // The blank second answer is dropped.
    assert_eq!(aggregated[1].answers, vec!["More breaks"]);
}

#[test]
fn aggregate_answers_stringifies_numeric_cells() {
    let rows = vec![row(&[("score", "9")])];
    // Numeric cells arrive as JSON numbers from the sheet; project them as text.
    let numeric = GoogleSheetsRowMatch {
        row_number: 3,
        values: json!({ "score": 7 }),
    };
    let rows = [rows, vec![numeric]].concat();
    let aggregated = aggregate_answers(&rows, &["score".to_string()]);
    assert_eq!(aggregated[0].answers, vec!["9", "7"]);
}

#[test]
fn build_prompt_numbers_questions_and_joins_answers() {
    let aggregated = vec![
        QuestionAnswers {
            question: "What went great?".to_string(),
            answers: vec!["Great speakers".to_string(), "Good food".to_string()],
        },
        QuestionAnswers {
            question: "How can we improve?".to_string(),
            answers: vec!["More breaks".to_string()],
        },
    ];
    let prompt = build_prompt(&aggregated);
    assert!(prompt.contains("1. What went great?: ```Great speakers | Good food```"));
    assert!(prompt.contains("2. How can we improve?: ```More breaks```"));
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
        .expect("feedback_trigger requirement present");
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

    // Read-only sheet read: http_read only.
    assert!(hints("load_feedback").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(!hints("load_feedback").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // LLM completion: a remote POST routed through http_write (billed; the
    // delivery gate must see it), never http_read.
    assert!(hints("summarize_feedback").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("summarize_feedback").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Gmail send: http_write only.
    assert!(hints("send_report").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("send_report").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Terminal KV upsert.
    assert!(hints("record_summary").contains(&EffectHint::KvWrite.as_str().to_string()));

    // Connector nodes carry connector-prefixed identifiers so the runtime can
    // infer each connection scope (sheets vs llm vs gmail).
    let identifier = |alias: &str| {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .map(|n| n.identifier.clone())
            .expect("node present")
    };
    assert_eq!(
        identifier("load_feedback"),
        "connector.google.sheets.load_feedback"
    );
    assert_eq!(
        identifier("summarize_feedback"),
        "connector.llm.summarize_feedback"
    );
    assert_eq!(
        identifier("send_report"),
        "connector.google.gmail.send_report"
    );
}

// ---- record_summary honesty + idempotency (s17 pattern) --------------------

fn kv_bag() -> (Arc<MemoryKv>, Arc<dyn ResourceAccess>) {
    let kv = Arc::new(MemoryKv::new());
    let bag: Arc<dyn ResourceAccess> = Arc::new(ResourceBag::default().with_kv(kv.clone()));
    (kv, bag)
}

fn sample_outcome(run_id: &str) -> SendOutcome {
    SendOutcome {
        request: sample_request(run_id),
        message_id: "msg-1".to_string(),
        response_count: 2,
        total_tokens: 60,
    }
}

#[tokio::test]
async fn record_summary_succeeds_under_exactly_declared_kv_hints() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_summary",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let record = context::with_resources(view, async {
        record_summary(sample_outcome("run-1"))
            .await
            .expect("write succeeds under declared hints")
    })
    .await;

    assert!(record.stored);
    assert_eq!(record.response_count, 2);
    assert_eq!(record.total_tokens, 60);
    assert!(
        scoped.take_denials().is_empty(),
        "declared hints must be sufficient: no CAP110 denials"
    );
}

#[tokio::test]
async fn record_summary_denied_without_kv_grant() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new("record_summary", bag, []));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        record_summary(sample_outcome("run-1"))
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
async fn record_summary_is_idempotent_on_run_id() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_summary",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));

    let first = context::with_resources(scoped.clone(), async {
        record_summary(sample_outcome("run-1"))
            .await
            .expect("first")
    })
    .await;
    let second = context::with_resources(scoped.clone(), async {
        record_summary(sample_outcome("run-1"))
            .await
            .expect("redelivery")
    })
    .await;

    assert!(first.stored, "first delivery writes the record");
    assert!(!second.stored, "redelivery is deduplicated");
    assert_eq!(first.key, second.key);

    let other = context::with_resources(scoped.clone(), async {
        record_summary(sample_outcome("run-2"))
            .await
            .expect("distinct run")
    })
    .await;
    assert!(other.stored, "a distinct run writes its own record");
    assert_ne!(first.key, other.key);
}

// ---- Compose: request -> read -> aggregate -> summarize -> send -> record ---

// The env lock is held across awaits deliberately: the process-global
// endpoint/auth env vars must not be mutated by a parallel test mid-run.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn manual_run_summarizes_feedback_emails_report_and_records_idempotently() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _sheets = EnvGuard::set(SHEETS_ENDPOINT_ENV, &server.base_url());
    let _gmail = EnvGuard::set(GMAIL_ENDPOINT_ENV, &server.base_url());
    let _llm = EnvGuard::set(LLM_ENDPOINT_ENV, &server.base_url());
    let _google_auth = EnvGuard::set(GOOGLE_AUTH_ENV, "s25-google-token");
    let _llm_auth = EnvGuard::set(LLM_AUTH_ENV, "s25-llm-key");

    // 1. sheets find_rows reads the form responses.
    let values_read = server.mock(|when, then| {
        when.method(GET)
            .path_contains("/values/")
            .header("authorization", "Bearer s25-google-token");
        then.status(200).json_body_obj(&feedback_values());
    });

    // 2. llm.complete: openai_compat POSTs /chat/completions with the LLM key.
    let llm_complete = server.mock(|when, then| {
        when.method(POST)
            .path("/chat/completions")
            .header("authorization", "Bearer s25-llm-key");
        then.status(200)
            .json_body(mock_completion_body("## Summary\nOverall positive."));
    });

    // 3. gmail send for the report email (Google-authed).
    let gmail_send = server.mock(|when, then| {
        when.method(POST)
            .path("/gmail/v1/users/me/messages/send")
            .header("authorization", "Bearer s25-google-token");
        then.status(200).json_body_obj(&json!({
            "id": "msg-report-1", "threadId": "thread-1", "labelIds": ["SENT"]
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

    let request = serde_json::to_value(sample_request("run-42")).expect("serialize request");
    let fire = || Invocation::new(TRIGGER_ALIAS, "capture", request.clone());

    let first = runtime.execute(fire()).await.expect("first run");
    let first = match first {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<SummaryRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the first run"),
    };
    assert!(first.stored);
    assert_eq!(first.response_count, 2);
    assert_eq!(first.total_tokens, 60);
    assert_eq!(first.message_id, "msg-report-1");
    assert_eq!(first.key, feedback_key("run-42"));

    values_read.assert_hits(1);
    llm_complete.assert_hits(1);
    gmail_send.assert_hits(1);

    // Redelivery of the SAME run: the read/summarize/send replay, but the
    // terminal KV record dedupes to one row.
    let second = runtime.execute(fire()).await.expect("redelivery runs");
    let second = match second {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<SummaryRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the redelivery"),
    };
    assert!(
        !second.stored,
        "redelivery must not write a duplicate record"
    );
    assert_eq!(second.key, first.key);
    gmail_send.assert_hits(2);

    let raw = kv
        .get(&feedback_key("run-42"))
        .await
        .expect("kv get")
        .expect("record present");
    let row: SummaryRecord = serde_json::from_slice(&raw).expect("decode row");
    assert_eq!(row.response_count, 2);
}
