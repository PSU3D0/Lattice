//! Canned-transport runtime contract for `connector.telegram.send_message`:
//! success path with full request assertions (the bot token spliced into the
//! `/bot<token>/sendMessage` path, the composed `{chat_id,text}` body), API
//! error mapping, auth misconfiguration, and connector-op reuse from a custom
//! `def_node`.

use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_telegram::runtime::errors::ConnectorRuntimeError;
use connector_telegram::runtime::transport::EnvConnectorRuntime;
use connector_telegram::{TelegramSendMessageInput, telegram_send_message};
use dag_core::{Effects, NodeError, NodeResult};
use dag_macros::def_node;
use httpmock::Method::POST;
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_TELEGRAM_BOT_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_TELEGRAM_BOT_AUTH";
const TOKEN: &str = "123456:ABCDEF";

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
                "connector.telegram.test",
                "connector.telegram",
            )),
    )
}

fn sample_input() -> TelegramSendMessageInput {
    TelegramSendMessageInput {
        chat_id: "1001".to_string(),
        text: "Hello from Lattice".to_string(),
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct MaybeNotifyInput {
    should_send: bool,
    chat_id: String,
    text: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct MaybeNotifyOutput {
    sent: bool,
    message_id: Option<i64>,
}

#[def_node(
    name = "MaybeNotify",
    summary = "Custom node that reuses the Telegram send-message connector operation",
    connector_ops(connector_telegram::ops::TelegramSendMessage)
)]
async fn maybe_notify(input: MaybeNotifyInput) -> NodeResult<MaybeNotifyOutput> {
    if !input.should_send {
        return Ok(MaybeNotifyOutput {
            sent: false,
            message_id: None,
        });
    }

    let sent = connector_telegram::ops::TelegramSendMessage::invoke(&TelegramSendMessageInput {
        chat_id: input.chat_id,
        text: input.text,
    })
    .await
    .map_err(|err| NodeError::new(err.to_string()))?;

    Ok(MaybeNotifyOutput {
        sent: true,
        message_id: Some(sent.message_id),
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
            .any(|op| op.operation_id == "connector.telegram.send_message")
    );

    let generated = connector_telegram::telegram_send_message_node_spec();
    assert_eq!(generated.effects, Effects::Effectful);
    assert!(
        generated
            .effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
}

#[tokio::test]
async fn send_message_posts_composed_body_to_token_path() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, TOKEN);

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path(format!("/bot{TOKEN}/sendMessage"))
            .header("content-type", "application/json")
            .json_body_obj(&serde_json::json!({ "chat_id": "1001", "text": "Hello from Lattice" }));
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": true,
            "result": { "message_id": 7, "date": 1_700_000_000, "text": "Hello from Lattice" }
        }));
    });

    let output = context::with_resources(http_resources(), async {
        connector_telegram::ops::TelegramSendMessage::invoke(&sample_input())
            .await
            .expect("send succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.message_id, 7);
    assert_eq!(output.date, 1_700_000_000);
    assert_eq!(output.text.as_deref(), Some("Hello from Lattice"));
}

#[tokio::test]
async fn custom_node_reuses_connector_operation() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, TOKEN);

    let mock = server.mock(|when, then| {
        when.method(POST).path_contains("/sendMessage");
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": true, "result": { "message_id": 9, "date": 1 }
        }));
    });

    let output = context::with_resources(http_resources(), async {
        maybe_notify(MaybeNotifyInput {
            should_send: true,
            chat_id: "2002".to_string(),
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
            message_id: Some(9),
        }
    );
}

#[tokio::test]
async fn send_message_maps_api_error_status_and_body() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, TOKEN);

    let mock = server.mock(|when, then| {
        when.method(POST).path_contains("/sendMessage");
        then.status(400).json_body_obj(&serde_json::json!({
            "ok": false, "error_code": 400, "description": "Bad Request: chat not found"
        }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_telegram::ops::TelegramSendMessage::invoke(&sample_input())
            .await
            .expect_err("bad request must fail")
    })
    .await;

    mock.assert();
    match err {
        ConnectorRuntimeError::HttpStatus { status, body } => {
            assert_eq!(status, 400);
            assert!(body.contains("chat not found"), "got: {body}");
        }
        other => panic!("expected HttpStatus error, got: {other}"),
    }

    // The def_node action wrapper keeps status + body visible in the NodeError.
    let node_err = context::with_resources(http_resources(), async {
        telegram_send_message(sample_input())
            .await
            .expect_err("bad request must fail through the action")
    })
    .await;
    let message = node_err.to_string();
    assert!(message.contains("400"), "got: {message}");
    assert!(message.contains("chat not found"), "got: {message}");
}

#[tokio::test]
async fn send_message_with_missing_auth_fails_actionably_without_calling_api() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(POST).path_contains("/sendMessage");
        then.status(200).json_body_obj(
            &serde_json::json!({ "ok": true, "result": { "message_id": 1, "date": 1 } }),
        );
    });

    let err = context::with_resources(http_resources(), async {
        connector_telegram::ops::TelegramSendMessage::invoke(&sample_input())
            .await
            .expect_err("missing auth must fail")
    })
    .await;

    // Nothing left the process, and the failure names role + env var.
    assert_eq!(mock.hits(), 0);
    let message = err.to_string();
    assert!(message.contains("telegram_bot_auth"), "got: {message}");
    assert!(message.contains(AUTH_ENV), "got: {message}");
}
