use super::*;

use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::ResourceBag;
use connector_slack_core::runtime::transport::EnvConnectorRuntime;
use dag_core::EffectHint;
use dag_core::requirements::TriggerKind;
use host_inproc::FlowBundle;
use httpmock::Method::{GET, POST, PUT};
use httpmock::MockServer;
use kernel_exec::ExecutionResult;
use kernel_plan::derive_requirements;

// ---- Env plumbing ----------------------------------------------------------

const SHEETS_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_SHEETS_DEFAULT_BASE_URL";
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

fn submission() -> SignupSubmission {
    SignupSubmission {
        email: "ada@newsletter.test".to_string(),
        first_name: "Ada".to_string(),
        last_name: "Lovelace".to_string(),
        job_level: "Director".to_string(),
        product_goals: "automate the boring parts".to_string(),
    }
}

// ---- Pure helper -----------------------------------------------------------

#[test]
fn notification_text_is_a_pure_function_of_the_submission() {
    assert_eq!(
        notification_text(&submission()),
        "ada@newsletter.test just signed up to the newsletter!"
    );
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
        .expect("signup_trigger requirement present");
    assert_eq!(trigger.kind, TriggerKind::Http);

    let entrypoint = requirements
        .entrypoints
        .iter()
        .find(|e| e.trigger_alias == TRIGGER_ALIAS)
        .expect("http entrypoint requirement present");
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

    // Append reads the header row before writing: http_read + http_write.
    assert!(hints("record_signup").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(hints("record_signup").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // Slack post: http_write only.
    assert!(hints("notify_signup").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("notify_signup").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Upsert reads then writes: http_read + http_write.
    assert!(hints("enrich_signup").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(hints("enrich_signup").contains(&EffectHint::HttpWrite.as_str().to_string()));

    // Every connector node carries a connector-prefixed identifier so the
    // runtime can infer the connection scope.
    let identifier = |alias: &str| {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .map(|n| n.identifier.clone())
            .expect("node present")
    };
    assert_eq!(
        identifier("record_signup"),
        "connector.google.sheets.record_signup"
    );
    assert_eq!(
        identifier("notify_signup"),
        "connector.slack.core.notify_signup"
    );
    assert_eq!(
        identifier("enrich_signup"),
        "connector.google.sheets.enrich_signup"
    );
}

// ---- Compose: webhook -> append -> slack -> upsert -> capture ---------------

async fn run_flow(bundle: FlowBundle, input: SignupSubmission) -> SignupRecord {
    let entrypoint = bundle.entrypoints.first().expect("entrypoint");
    let http = Arc::new(ReqwestHttpClient::default());
    let bag = ResourceBag::default()
        .with_http_read(Arc::clone(&http))
        .with_http_write(http)
        .with_connector_runtime(Arc::new(EnvConnectorRuntime));

    let payload = serde_json::to_value(&input).expect("serialize submission");
    let result = bundle
        .executor()
        .with_resource_bag(bag)
        .run_once(
            &bundle.validated_ir,
            entrypoint.trigger_alias.as_str(),
            payload,
            entrypoint.capture_alias.as_str(),
            entrypoint.deadline,
        )
        .await
        .expect("flow runs");

    match result {
        ExecutionResult::Value(value) => {
            serde_json::from_value(value).expect("decode signup record")
        }
        ExecutionResult::Stream(_) => panic!("expected a value result"),
        ExecutionResult::Halt { alias, .. } => panic!("unexpected halt at {alias}"),
    }
}

// The env lock is held across awaits deliberately: the process-global
// endpoint/auth env vars must not be mutated by a parallel test mid-fire.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn webhook_signup_appends_notifies_and_enriches() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _sheets = EnvGuard::set(SHEETS_ENDPOINT_ENV, &server.base_url());
    let _slack = EnvGuard::set(SLACK_ENDPOINT_ENV, &server.base_url());
    let _google_auth = EnvGuard::set(GOOGLE_AUTH_ENV, "s20-google-token");
    let _slack_auth = EnvGuard::set(SLACK_AUTH_ENV, "s20-slack-token");

    // Header read: the table already contains the signup's row so the upsert
    // takes the update (PUT) path.
    let values_read = server.mock(|when, then| {
        when.method(GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [
                ["email", "first_name", "last_name", "job_level", "product_goals"],
                ["ada@newsletter.test", "Ada", "Lovelace", "", ""]
            ]
        }));
    });
    // append_row writes the new contact row.
    let values_append = server.mock(|when, then| {
        when.method(POST).path_contains("append");
        then.status(200).json_body_obj(&serde_json::json!({
            "updates": { "updatedRange": "'Sheet1'!A2:E2" }
        }));
    });
    // Slack notification.
    let slack_post = server.mock(|when, then| {
        when.method(POST)
            .path("/chat.postMessage")
            .header("authorization", "Bearer s20-slack-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": true, "channel": "C-newsletter", "ts": "1700000000.000200"
        }));
    });
    // upsert_row updates the matched row.
    let values_update = server.mock(|when, then| {
        when.method(PUT).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "updatedRange": "'Sheet1'!A2:E2"
        }));
    });

    let record = run_flow(bundle(), submission()).await;

    // append reads once + upsert reads once = two header reads.
    values_read.assert_hits(2);
    values_append.assert_hits(1);
    slack_post.assert_hits(1);
    values_update.assert_hits(1);

    assert_eq!(record.email, "ada@newsletter.test");
    assert_eq!(record.appended_range, "'Sheet1'!A2:E2");
    assert_eq!(record.updated_range, "'Sheet1'!A2:E2");
    assert_eq!(record.slack_channel, "C-newsletter");
    assert_eq!(record.slack_ts, "1700000000.000200");
}
