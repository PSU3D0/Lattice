//! Canned-transport runtime contract for `connector.slack.core.post_message`:
//! success path with full request assertions (path, bearer auth, composed JSON
//! body), Slack `ok:false` envelope mapping, HTTP error mapping, auth
//! misconfiguration, and connector-op reuse from a custom `def_node`.
//!
//! `ENV_LOCK` is held across awaits deliberately to serialize the
//! process-global endpoint/auth env vars (same posture as the gmail harness).
#![allow(clippy::await_holding_lock)]

use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_slack_core::runtime::errors::ConnectorRuntimeError;
use connector_slack_core::runtime::transport::EnvConnectorRuntime;
use connector_slack_core::{SlackPostMessageInput, slack_post_message};
use dag_core::{Effects, NodeError, NodeResult};
use dag_macros::def_node;
use httpmock::Method::POST;
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_SLACK_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_SLACK_AUTH";

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
                "connector.slack.core.test",
                "connector.slack.core",
            )),
    )
}

fn sample_input() -> SlackPostMessageInput {
    SlackPostMessageInput {
        channel: "#general".to_string(),
        text: "a new signup just arrived".to_string(),
        blocks: None,
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct MaybeNotifyInput {
    should_send: bool,
    channel: String,
    text: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct MaybeNotifyOutput {
    sent: bool,
    ts: Option<String>,
}

#[def_node(
    name = "MaybeNotify",
    summary = "Custom node that reuses the Slack post-message connector operation",
    connector_ops(connector_slack_core::ops::SlackPostMessage)
)]
async fn maybe_notify(input: MaybeNotifyInput) -> NodeResult<MaybeNotifyOutput> {
    if !input.should_send {
        return Ok(MaybeNotifyOutput {
            sent: false,
            ts: None,
        });
    }

    let posted = connector_slack_core::ops::SlackPostMessage::invoke(&SlackPostMessageInput {
        channel: input.channel,
        text: input.text,
        blocks: None,
    })
    .await
    .map_err(|err| NodeError::new(err.to_string()))?;

    Ok(MaybeNotifyOutput {
        sent: true,
        ts: Some(posted.ts),
    })
}

#[test]
fn custom_node_spec_auto_hoists_connector_op_requirements() {
    let spec = maybe_notify_node_spec();
    assert_eq!(spec.effects, Effects::Effectful);
    assert_eq!(spec.determinism, dag_core::Determinism::BestEffort);
    assert!(
        spec.effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
    assert!(
        spec.connector_ops
            .iter()
            .any(|op| op.operation_id == "connector.slack.core.post_message")
    );

    let generated = connector_slack_core::slack_post_message_node_spec();
    assert_eq!(generated.effects, Effects::Effectful);
    assert!(
        generated
            .effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
}

#[tokio::test]
async fn post_message_posts_composed_body_with_bearer_auth() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "slack-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/chat.postMessage")
            .header("accept", "application/json")
            .header("content-type", "application/json; charset=utf-8")
            .header("authorization", "Bearer slack-secret-token")
            .json_body_obj(&serde_json::json!({
                "channel": "#general",
                "text": "a new signup just arrived"
            }));
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": true,
            "channel": "C123",
            "ts": "1503435956.000247"
        }));
    });

    let output = context::with_resources(http_resources(), async {
        connector_slack_core::ops::SlackPostMessage::invoke(&sample_input())
            .await
            .expect("post succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.channel, "C123");
    assert_eq!(output.ts, "1503435956.000247");
}

#[tokio::test]
async fn post_message_maps_ok_false_envelope() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "slack-secret-token");

    // Slack signals logical failure with HTTP 200 + `ok:false`.
    let mock = server.mock(|when, then| {
        when.method(POST).path("/chat.postMessage");
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": false,
            "error": "channel_not_found"
        }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_slack_core::ops::SlackPostMessage::invoke(&sample_input())
            .await
            .expect_err("ok:false must fail")
    })
    .await;

    mock.assert();
    match err {
        ConnectorRuntimeError::InvalidResponse(message) => {
            assert!(message.contains("channel_not_found"), "got: {message}");
        }
        other => panic!("expected InvalidResponse error, got: {other}"),
    }
}

#[tokio::test]
async fn post_message_maps_http_error_status_and_body() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "slack-secret-token");

    // Slack rate-limit: a non-2xx status surfaces through the shared transport.
    let mock = server.mock(|when, then| {
        when.method(POST).path("/chat.postMessage");
        then.status(429).json_body_obj(&serde_json::json!({
            "ok": false,
            "error": "ratelimited"
        }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_slack_core::ops::SlackPostMessage::invoke(&sample_input())
            .await
            .expect_err("429 must fail")
    })
    .await;

    mock.assert();
    match err {
        ConnectorRuntimeError::HttpStatus { status, body } => {
            assert_eq!(status, 429);
            assert!(body.contains("ratelimited"), "got: {body}");
        }
        other => panic!("expected HttpStatus error, got: {other}"),
    }

    // The def_node action wrapper keeps the status visible in the NodeError.
    let node_err = context::with_resources(http_resources(), async {
        slack_post_message(sample_input())
            .await
            .expect_err("429 must fail through the action")
    })
    .await;
    assert!(node_err.to_string().contains("429"), "got: {node_err}");
}

#[tokio::test]
async fn post_message_with_missing_auth_fails_actionably_without_calling_api() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(POST).path("/chat.postMessage");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "ok": true, "channel": "C", "ts": "1" }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_slack_core::ops::SlackPostMessage::invoke(&sample_input())
            .await
            .expect_err("missing auth must fail")
    })
    .await;

    // Nothing left the process, and the failure names role + env var.
    assert_eq!(mock.hits(), 0);
    let message = err.to_string();
    assert!(message.contains("slack_auth"), "got: {message}");
    assert!(message.contains(AUTH_ENV), "got: {message}");
}

#[tokio::test]
async fn custom_node_reuses_connector_operation() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "slack-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST).path("/chat.postMessage");
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": true, "channel": "C999", "ts": "42.0"
        }));
    });

    let output = context::with_resources(http_resources(), async {
        maybe_notify(MaybeNotifyInput {
            should_send: true,
            channel: "#alerts".to_string(),
            text: "hi".to_string(),
        })
        .await
        .expect("custom node succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(
        output,
        MaybeNotifyOutput {
            sent: true,
            ts: Some("42.0".to_string()),
        }
    );
}
