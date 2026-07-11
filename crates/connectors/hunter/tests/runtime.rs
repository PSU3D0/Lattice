//! Canned-transport runtime contract for `connector.hunter.verify_email`:
//! success path with full request assertions (the api key spliced into the
//! `api_key` query parameter, the `email` query parameter, the `{data:{...}}`
//! envelope decode), API error mapping, auth misconfiguration, and connector-op
//! reuse from a custom `def_node`.

use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_hunter::runtime::errors::ConnectorRuntimeError;
use connector_hunter::runtime::transport::EnvConnectorRuntime;
use connector_hunter::{HunterVerifyEmailInput, hunter_verify_email};
use dag_core::{Effects, NodeError, NodeResult};
use dag_macros::def_node;
use httpmock::Method::GET;
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_HUNTER_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_HUNTER_API_KEY_AUTH";
const API_KEY: &str = "hunter-secret-key";

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }

    fn remove(key: &'static str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(previous) => unsafe {
                std::env::set_var(self.key, previous);
            },
            None => unsafe {
                std::env::remove_var(self.key);
            },
        }
    }
}

fn http_resources() -> Arc<ResourceBag> {
    let client = Arc::new(ReqwestHttpClient::default());
    Arc::new(
        ResourceBag::default()
            .with_http_read(Arc::clone(&client))
            .with_http_write(client)
            .with_connector_runtime(Arc::new(EnvConnectorRuntime))
            .with_connector_scope(capabilities::connector::ConnectorBindingScope::new(
                "flow://tests",
                "runtime_test",
                "connector.hunter.test",
                "connector.hunter",
            )),
    )
}

fn sample_input() -> HunterVerifyEmailInput {
    HunterVerifyEmailInput {
        email: "ada@leads.test".to_string(),
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct GateInput {
    email: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct GateOutput {
    deliverable: bool,
    score: i64,
}

#[def_node(
    name = "GateOnVerification",
    summary = "Custom node that reuses the Hunter verify-email connector operation",
    connector_ops(connector_hunter::ops::HunterVerifyEmail)
)]
async fn gate_on_verification(input: GateInput) -> NodeResult<GateOutput> {
    let verdict = connector_hunter::ops::HunterVerifyEmail::invoke(&HunterVerifyEmailInput {
        email: input.email,
    })
    .await
    .map_err(|err| NodeError::new(err.to_string()))?;

    Ok(GateOutput {
        deliverable: verdict.result == "deliverable",
        score: verdict.score,
    })
}

#[test]
fn custom_node_spec_auto_hoists_connector_op_requirements() {
    let spec = gate_on_verification_node_spec();
    assert_eq!(spec.effects, Effects::ReadOnly);
    assert_eq!(spec.determinism, dag_core::Determinism::BestEffort);
    assert!(
        spec.effect_hints
            .contains(&capabilities::http::HINT_HTTP_READ)
    );
    assert!(
        spec.connector_ops
            .iter()
            .any(|op| op.operation_id == "connector.hunter.verify_email")
    );

    let generated = connector_hunter::hunter_verify_email_node_spec();
    assert_eq!(generated.effects, Effects::ReadOnly);
    assert!(
        generated
            .effect_hints
            .contains(&capabilities::http::HINT_HTTP_READ)
    );
}

#[tokio::test]
async fn verify_email_sends_query_params_and_decodes_envelope() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, API_KEY);

    let mock = server.mock(|when, then| {
        when.method(GET)
            .path("/v2/email-verifier")
            .query_param("email", "ada@leads.test")
            .query_param("api_key", API_KEY);
        then.status(200).json_body_obj(&serde_json::json!({
            "data": {
                "status": "valid",
                "result": "deliverable",
                "score": 91,
                "email": "ada@leads.test"
            },
            "meta": { "params": { "email": "ada@leads.test" } }
        }));
    });

    let output = context::with_resources(http_resources(), async {
        connector_hunter::ops::HunterVerifyEmail::invoke(&sample_input())
            .await
            .expect("verify succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.email, "ada@leads.test");
    assert_eq!(output.status, "valid");
    assert_eq!(output.result, "deliverable");
    assert_eq!(output.score, 91);
}

#[tokio::test]
async fn custom_node_reuses_connector_operation() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, API_KEY);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/v2/email-verifier");
        then.status(200).json_body_obj(&serde_json::json!({
            "data": { "status": "invalid", "result": "undeliverable", "score": 0, "email": "x@y.z" }
        }));
    });

    let output = context::with_resources(http_resources(), async {
        gate_on_verification(GateInput {
            email: "x@y.z".to_string(),
        })
        .await
        .expect("custom node succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(
        output,
        GateOutput {
            deliverable: false,
            score: 0,
        }
    );
}

#[tokio::test]
async fn verify_email_maps_api_error_status_and_body() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, API_KEY);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/v2/email-verifier");
        then.status(401).json_body_obj(&serde_json::json!({
            "errors": [{ "id": "unauthorized", "details": "Invalid API key" }]
        }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_hunter::ops::HunterVerifyEmail::invoke(&sample_input())
            .await
            .expect_err("bad key must fail")
    })
    .await;

    mock.assert();
    match err {
        ConnectorRuntimeError::HttpStatus { status, body } => {
            assert_eq!(status, 401);
            assert!(body.contains("Invalid API key"), "got: {body}");
        }
        other => panic!("expected HttpStatus error, got: {other}"),
    }

    let node_err = context::with_resources(http_resources(), async {
        hunter_verify_email(sample_input())
            .await
            .expect_err("bad key must fail through the action")
    })
    .await;
    let message = node_err.to_string();
    assert!(message.contains("401"), "got: {message}");
    assert!(message.contains("Invalid API key"), "got: {message}");
}

#[tokio::test]
async fn verify_email_with_missing_auth_fails_actionably_without_calling_api() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/v2/email-verifier");
        then.status(200).json_body_obj(&serde_json::json!({
            "data": { "status": "valid", "result": "deliverable", "score": 90, "email": "a@b.c" }
        }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_hunter::ops::HunterVerifyEmail::invoke(&sample_input())
            .await
            .expect_err("missing auth must fail")
    })
    .await;

    assert_eq!(mock.hits(), 0);
    let message = err.to_string();
    assert!(message.contains("hunter_api_key_auth"), "got: {message}");
    assert!(message.contains(AUTH_ENV), "got: {message}");
}
