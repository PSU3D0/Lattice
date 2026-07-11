//! Canned-transport runtime contract for `connector.google.gmail.send_message`:
//! success path with full request assertions (path, bearer auth, composed
//! `raw` payload), API error mapping, auth misconfiguration, and connector-op
//! reuse from a custom `def_node`.

use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_google_gmail::runtime::errors::ConnectorRuntimeError;
use connector_google_gmail::runtime::transport::EnvConnectorRuntime;
use connector_google_gmail::{GoogleGmailSendMessageInput, google_gmail_send_message};
use connector_google_platform::gmail::{base64url_no_pad, build_plain_text_email};
use dag_core::{Effects, NodeError, NodeResult};
use dag_macros::def_node;
use httpmock::Method::POST;
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_GMAIL_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";

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
                "connector.google.gmail.test",
                "connector.google.gmail",
            )),
    )
}

fn sample_input() -> GoogleGmailSendMessageInput {
    GoogleGmailSendMessageInput {
        to: "secops@example.test".to_string(),
        cc: None,
        bcc: None,
        subject: "Audit summary".to_string(),
        text_body: "two files flagged".to_string(),
    }
}

/// The exact `raw` payload the op must produce for `sample_input()` — composed
/// with the platform helpers, which are pinned to RFC test vectors in their
/// own unit tests.
fn expected_raw() -> String {
    base64url_no_pad(
        build_plain_text_email(
            "secops@example.test",
            None,
            None,
            "Audit summary",
            "two files flagged",
        )
        .as_bytes(),
    )
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct MaybeNotifyInput {
    should_send: bool,
    to: String,
    subject: String,
    text_body: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct MaybeNotifyOutput {
    sent: bool,
    message_id: Option<String>,
}

#[def_node(
    name = "MaybeNotify",
    summary = "Custom node that reuses the Gmail send-message connector operation",
    connector_ops(connector_google_gmail::ops::GoogleGmailSendMessage)
)]
async fn maybe_notify(input: MaybeNotifyInput) -> NodeResult<MaybeNotifyOutput> {
    if !input.should_send {
        return Ok(MaybeNotifyOutput {
            sent: false,
            message_id: None,
        });
    }

    let sent =
        connector_google_gmail::ops::GoogleGmailSendMessage::invoke(&GoogleGmailSendMessageInput {
            to: input.to,
            cc: None,
            bcc: None,
            subject: input.subject,
            text_body: input.text_body,
        })
        .await
        .map_err(|err| NodeError::new(err.to_string()))?;

    Ok(MaybeNotifyOutput {
        sent: true,
        message_id: Some(sent.id),
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
            .any(|op| op.operation_id == "connector.google.gmail.send_message")
    );

    let generated = connector_google_gmail::google_gmail_send_message_node_spec();
    assert_eq!(generated.effects, Effects::Effectful);
    assert!(
        generated
            .effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
}

#[tokio::test]
async fn send_message_posts_composed_raw_payload_with_bearer_auth() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "gmail-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/gmail/v1/users/me/messages/send")
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .header("authorization", "Bearer gmail-secret-token")
            .json_body_obj(&serde_json::json!({ "raw": expected_raw() }));
        then.status(200).json_body_obj(&serde_json::json!({
            "id": "msg-1",
            "threadId": "thread-1",
            "labelIds": ["SENT"]
        }));
    });

    let output = context::with_resources(http_resources(), async {
        connector_google_gmail::ops::GoogleGmailSendMessage::invoke(&sample_input())
            .await
            .expect("send succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.id, "msg-1");
    assert_eq!(output.thread_id.as_deref(), Some("thread-1"));
    assert_eq!(output.label_ids, vec!["SENT".to_string()]);
}

#[tokio::test]
async fn custom_node_reuses_connector_operation() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "gmail-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST).path("/gmail/v1/users/me/messages/send");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "id": "msg-2" }));
    });

    let output = context::with_resources(http_resources(), async {
        maybe_notify(MaybeNotifyInput {
            should_send: true,
            to: "a@example.test".to_string(),
            subject: "hi".to_string(),
            text_body: "body".to_string(),
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
            message_id: Some("msg-2".to_string()),
        }
    );
}

#[tokio::test]
async fn send_message_maps_api_error_status_and_body() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "gmail-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST).path("/gmail/v1/users/me/messages/send");
        then.status(400).json_body_obj(&serde_json::json!({
            "error": { "code": 400, "message": "Invalid To header", "status": "INVALID_ARGUMENT" }
        }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_google_gmail::ops::GoogleGmailSendMessage::invoke(&sample_input())
            .await
            .expect_err("bad request must fail")
    })
    .await;

    mock.assert();
    match err {
        ConnectorRuntimeError::HttpStatus { status, body } => {
            assert_eq!(status, 400);
            assert!(body.contains("Invalid To header"), "got: {body}");
        }
        other => panic!("expected HttpStatus error, got: {other}"),
    }

    // The def_node action wrapper must keep both visible in the NodeError.
    let node_err = context::with_resources(http_resources(), async {
        google_gmail_send_message(sample_input())
            .await
            .expect_err("bad request must fail through the action")
    })
    .await;
    let message = node_err.to_string();
    assert!(message.contains("400"), "got: {message}");
    assert!(message.contains("Invalid To header"), "got: {message}");
}

#[tokio::test]
async fn send_message_with_missing_auth_fails_actionably_without_calling_api() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(POST).path("/gmail/v1/users/me/messages/send");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "id": "never" }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_google_gmail::ops::GoogleGmailSendMessage::invoke(&sample_input())
            .await
            .expect_err("missing auth must fail")
    })
    .await;

    // Nothing left the process, and the failure names role + env var.
    assert_eq!(mock.hits(), 0);
    let message = err.to_string();
    assert!(message.contains("google_workspace_auth"), "got: {message}");
    assert!(message.contains(AUTH_ENV), "got: {message}");
}
