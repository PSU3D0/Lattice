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

const SHEETS_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_SHEETS_DEFAULT_BASE_URL";
const SHEETS_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";
const LLM_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_LLM_DEFAULT_BASE_URL";
const LLM_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_LLM_API_KEY";
const SLACK_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_SLACK_DEFAULT_BASE_URL";
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

fn sample_request() -> ReminderRequest {
    ReminderRequest {
        reminder_id: "rem-001".to_string(),
        user: "darrell".to_string(),
        date: "2026-07-11".to_string(),
        target_ml: 2000.0,
        model: DEFAULT_MODEL.to_string(),
    }
}

fn request_json(request: &ReminderRequest) -> serde_json::Value {
    serde_json::to_value(request).expect("serialize reminder request")
}

fn sample_log(request: &ReminderRequest) -> LogRead {
    LogRead {
        request: request.clone(),
        rows: vec![
            GoogleSheetsRowMatch {
                row_number: 2,
                values: json!({ "date": "2026-07-11", "time": "08:15:00", "value": "250" }),
            },
            GoogleSheetsRowMatch {
                row_number: 3,
                values: json!({ "date": "2026-07-11", "time": "10:30:00", "value": "300" }),
            },
        ],
    }
}

/// Wire rows the sheet mock returns (header row + two intake rows).
fn sheet_values_body() -> serde_json::Value {
    json!({
        "values": [
            ["date", "time", "value"],
            ["2026-07-11", "08:15:00", "250"],
            ["2026-07-11", "10:30:00", "300"]
        ]
    })
}

/// OpenAI-compatible chat completion whose content is the model's JSON message.
fn mock_completion_body(message: &str) -> serde_json::Value {
    let content = json!({ "message": message }).to_string();
    json!({
        "id": "chatcmpl-s22",
        "object": "chat.completion",
        "created": 1,
        "model": "gpt-4o-mini",
        "system_fingerprint": null,
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": content, "tool_calls": [] },
            "logprobs": null,
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 42,
            "completion_tokens": 30,
            "total_tokens": 72,
            "prompt_tokens_details": { "cached_tokens": 0 }
        }
    })
}

// ---- Pure helpers ----------------------------------------------------------

#[test]
fn progress_bar_fills_by_fraction() {
    assert_eq!(progress_bar(0.0), "⬜".repeat(10));
    assert_eq!(progress_bar(1.0), "💧".repeat(10));
    // 0.275 -> round(2.75) = 3 filled cells.
    assert_eq!(
        progress_bar(0.275),
        format!("{}{}", "💧".repeat(3), "⬜".repeat(7))
    );
}

#[test]
fn summarize_folds_intake_rows() {
    let state = summarize(&sample_log(&sample_request()));
    assert_eq!(state.consumed_ml, 550.0);
    assert_eq!(state.drink_count, 2);
    assert_eq!(state.target_ml, 2000.0);
    assert_eq!(state.last_drink_time, "10:30:00");
    // 550 / 2000 = 0.275.
    assert!((state.progress - 0.275).abs() < 1e-9);
    assert_eq!(
        state.progress_bar,
        format!("{}{}", "💧".repeat(3), "⬜".repeat(7))
    );
}

#[test]
fn summarize_handles_empty_log() {
    let request = sample_request();
    let state = summarize(&LogRead {
        request,
        rows: Vec::new(),
    });
    assert_eq!(state.consumed_ml, 0.0);
    assert_eq!(state.drink_count, 0);
    assert_eq!(state.progress, 0.0);
    assert_eq!(state.last_drink_time, "none");
}

#[test]
fn reminder_prompt_mentions_status() {
    let state = summarize(&sample_log(&sample_request()));
    let prompt = reminder_prompt(&state);
    assert!(prompt.contains("darrell"));
    assert!(prompt.contains("2026-07-11"));
    assert!(prompt.contains("550"));
    assert!(prompt.contains("2000"));
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
        .expect("reminder trigger requirement present");
    assert_eq!(trigger.kind, TriggerKind::Http);

    let entrypoint = requirements
        .entrypoints
        .iter()
        .find(|e| e.trigger_alias == TRIGGER_ALIAS)
        .expect("http entrypoint requirement present");
    assert_eq!(entrypoint.capture_alias, "capture");
    assert_eq!(entrypoint.method.as_deref(), Some("POST"));
    assert_eq!(
        entrypoint.route_path.as_deref(),
        Some("/hydration-reminder")
    );
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

    // Sheets find_rows: read-only.
    assert!(hints("read_water_log").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(!hints("read_water_log").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // LLM completion: a remote POST -> http_write.
    assert!(hints("generate_reminder").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // Slack post: http_write.
    assert!(hints("post_reminder").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // Terminal KV upsert.
    assert!(hints("record_reminder").contains(&EffectHint::KvWrite.as_str().to_string()));

    // Every connector node carries its connector-prefixed op identifier.
    let identifier = |alias: &str| {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .map(|n| n.identifier.clone())
            .expect("node present")
    };
    assert_eq!(
        identifier("read_water_log"),
        "connector.google.sheets.read_water_log"
    );
    assert_eq!(
        identifier("generate_reminder"),
        "connector.llm.generate_reminder"
    );
    assert_eq!(
        identifier("post_reminder"),
        "connector.slack.core.post_reminder"
    );
}

// ---- record_reminder honesty + idempotency (s16/s18 pattern) ---------------

fn kv_bag() -> (Arc<MemoryKv>, Arc<dyn ResourceAccess>) {
    let kv = Arc::new(MemoryKv::new());
    let bag: Arc<dyn ResourceAccess> = Arc::new(ResourceBag::default().with_kv(kv.clone()));
    (kv, bag)
}

fn sample_posted(reminder_id: &str) -> PostedReminder {
    PostedReminder {
        reminder_id: reminder_id.to_string(),
        user: "darrell".to_string(),
        message: "Drink up!".to_string(),
        slack_channel: "C-hydration".to_string(),
        slack_ts: "1700000000.000100".to_string(),
    }
}

#[tokio::test]
async fn record_reminder_succeeds_under_exactly_declared_kv_hints() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_reminder",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let record = context::with_resources(view, async {
        record_reminder(sample_posted("rem-001"))
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
async fn record_reminder_denied_without_kv_grant() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new("record_reminder", bag, []));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        record_reminder(sample_posted("rem-001"))
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
async fn record_reminder_is_idempotent_on_reminder_id() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_reminder",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));

    let first = context::with_resources(scoped.clone(), async {
        record_reminder(sample_posted("rem-001"))
            .await
            .expect("first")
    })
    .await;
    let second = context::with_resources(scoped.clone(), async {
        record_reminder(sample_posted("rem-001"))
            .await
            .expect("redelivery")
    })
    .await;

    assert!(first.stored, "first delivery writes the record");
    assert!(!second.stored, "redelivery is deduplicated");
    assert_eq!(first.key, second.key);

    let other = context::with_resources(scoped.clone(), async {
        record_reminder(sample_posted("rem-999"))
            .await
            .expect("a distinct reminder")
    })
    .await;
    assert!(other.stored, "a distinct reminder writes its own record");
    assert_ne!(first.key, other.key);
}

// ---- Compose: webhook -> sheets -> summarize -> llm -> slack -> record ------

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn webhook_reads_log_generates_reminder_posts_slack_and_records_idempotent() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _sheets = EnvGuard::set(SHEETS_ENDPOINT_ENV, &server.base_url());
    let _llm = EnvGuard::set(LLM_ENDPOINT_ENV, &server.base_url());
    let _slack = EnvGuard::set(SLACK_ENDPOINT_ENV, &server.base_url());
    let _sheets_auth = EnvGuard::set(SHEETS_AUTH_ENV, "s22-google-token");
    let _llm_auth = EnvGuard::set(LLM_AUTH_ENV, "s22-llm-key");
    let _slack_auth = EnvGuard::set(SLACK_AUTH_ENV, "s22-slack-token");

    // 1. Sheets find_rows reads the day's intake table.
    let values_read = server.mock(|when, then| {
        when.method(GET)
            .path_contains("/values/")
            .header("authorization", "Bearer s22-google-token");
        then.status(200).json_body_obj(&sheet_values_body());
    });
    // 2. LLM completion returns the structured `{message}`.
    let llm_complete = server.mock(|when, then| {
        when.method(POST)
            .path("/chat/completions")
            .header("authorization", "Bearer s22-llm-key");
        then.status(200)
            .json_body(mock_completion_body("Drink up — your body will thank you!"));
    });
    // 3. Slack post.
    let slack_post = server.mock(|when, then| {
        when.method(POST)
            .path("/chat.postMessage")
            .header("authorization", "Bearer s22-slack-token");
        then.status(200).json_body_obj(&json!({
            "ok": true,
            "channel": "C-hydration-1",
            "ts": "1700000000.000200"
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

    let fire = || Invocation::new(TRIGGER_ALIAS, "capture", request_json(&sample_request()));

    // First delivery.
    let first = runtime.execute(fire()).await.expect("first delivery runs");
    let first = match first {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<ReminderRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the first delivery"),
    };
    assert!(first.stored);
    assert_eq!(first.reminder_id, "rem-001");
    assert_eq!(first.slack_channel, "C-hydration-1");
    assert_eq!(first.slack_ts, "1700000000.000200");
    assert_eq!(first.message, "Drink up — your body will thank you!");
    assert_eq!(first.key, reminder_key("rem-001"));

    values_read.assert_hits(1);
    llm_complete.assert_hits(1);
    slack_post.assert_hits(1);

    // Redelivery of the SAME reminder: the reads/writes replay and the terminal
    // KV record dedupes to a single row.
    let second = runtime.execute(fire()).await.expect("redelivery runs");
    let second = match second {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<ReminderRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the redelivery"),
    };
    assert!(
        !second.stored,
        "redelivery must not write a duplicate record"
    );
    assert_eq!(second.key, first.key);

    // Exactly one durable record for the reminder.
    let raw = kv
        .get(&reminder_key("rem-001"))
        .await
        .expect("kv get")
        .expect("record present");
    let row: ReminderRecord = serde_json::from_slice(&raw).expect("decode row");
    assert_eq!(row.slack_ts, "1700000000.000200");
}
