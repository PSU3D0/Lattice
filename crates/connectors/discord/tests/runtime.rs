//! Canned-transport runtime contract for `connector.discord.send_message`:
//! success path with full request assertions (the webhook id/token spliced into
//! the `/api/webhooks/<id>/<token>` path, the composed `{content,embeds}` body,
//! a 204 No Content response), API error mapping, auth misconfiguration, and
//! connector-op reuse from a custom `def_node`.

use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_discord::runtime::errors::ConnectorRuntimeError;
use connector_discord::runtime::transport::EnvConnectorRuntime;
use connector_discord::{DiscordEmbed, DiscordSendMessageInput, discord_send_message};
use dag_core::{Effects, NodeError, NodeResult};
use dag_macros::def_node;
use httpmock::Method::POST;
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_DISCORD_WEBHOOK_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_DISCORD_WEBHOOK_AUTH";
/// `<webhook_id>/<webhook_token>` — the whole pair is the resolved secret.
const TOKEN: &str = "123456789/aBcDeF-webhook-token";

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
                "connector.discord.test",
                "connector.discord",
            )),
    )
}

fn sample_input() -> DiscordSendMessageInput {
    DiscordSendMessageInput {
        content: None,
        embeds: vec![DiscordEmbed {
            title: Some("New Lead from Ada".to_string()),
            description: Some("Email: ada@leads.test".to_string()),
            color: Some(0x00_FF_F2),
            author_name: Some("Lattice Automation".to_string()),
        }],
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct MaybeAnnounceInput {
    should_send: bool,
    title: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct MaybeAnnounceOutput {
    sent: bool,
}

#[def_node(
    name = "MaybeAnnounce",
    summary = "Custom node that reuses the Discord send-message connector operation",
    connector_ops(connector_discord::ops::DiscordSendMessage)
)]
async fn maybe_announce(input: MaybeAnnounceInput) -> NodeResult<MaybeAnnounceOutput> {
    if !input.should_send {
        return Ok(MaybeAnnounceOutput { sent: false });
    }

    connector_discord::ops::DiscordSendMessage::invoke(&DiscordSendMessageInput {
        content: None,
        embeds: vec![DiscordEmbed {
            title: Some(input.title),
            ..DiscordEmbed::default()
        }],
    })
    .await
    .map_err(|err| NodeError::new(err.to_string()))?;

    Ok(MaybeAnnounceOutput { sent: true })
}

#[test]
fn custom_node_spec_auto_hoists_connector_op_requirements() {
    let spec = maybe_announce_node_spec();
    assert_eq!(spec.effects, Effects::Effectful);
    assert_eq!(spec.determinism, dag_core::Determinism::BestEffort);
    assert!(
        spec.effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
    assert!(
        spec.connector_ops
            .iter()
            .any(|op| op.operation_id == "connector.discord.send_message")
    );

    let generated = connector_discord::discord_send_message_node_spec();
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
            .path(format!("/api/webhooks/{TOKEN}"))
            .header("content-type", "application/json")
            .json_body_obj(&serde_json::json!({
                "embeds": [{
                    "title": "New Lead from Ada",
                    "description": "Email: ada@leads.test",
                    "color": 0x00_FF_F2,
                    "author": { "name": "Lattice Automation" }
                }]
            }));
        then.status(204);
    });

    let output = context::with_resources(http_resources(), async {
        connector_discord::ops::DiscordSendMessage::invoke(&sample_input())
            .await
            .expect("send succeeds")
    })
    .await;

    mock.assert();
    assert!(output.delivered);
    assert_eq!(output.message_id, None);
}

#[tokio::test]
async fn send_message_surfaces_message_id_when_wait_true_body_present() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, TOKEN);

    let mock = server.mock(|when, then| {
        when.method(POST).path_contains("/api/webhooks/");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "id": "555000111" }));
    });

    let output = context::with_resources(http_resources(), async {
        connector_discord::ops::DiscordSendMessage::invoke(&sample_input())
            .await
            .expect("send succeeds")
    })
    .await;

    mock.assert();
    assert!(output.delivered);
    assert_eq!(output.message_id.as_deref(), Some("555000111"));
}

#[tokio::test]
async fn custom_node_reuses_connector_operation() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, TOKEN);

    let mock = server.mock(|when, then| {
        when.method(POST).path_contains("/api/webhooks/");
        then.status(204);
    });

    let output = context::with_resources(http_resources(), async {
        maybe_announce(MaybeAnnounceInput {
            should_send: true,
            title: "hi".to_string(),
        })
        .await
        .expect("custom node succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output, MaybeAnnounceOutput { sent: true });
}

#[tokio::test]
async fn send_message_maps_api_error_status_and_body() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, TOKEN);

    let mock = server.mock(|when, then| {
        when.method(POST).path_contains("/api/webhooks/");
        then.status(401).json_body_obj(
            &serde_json::json!({ "message": "Invalid Webhook Token", "code": 50027 }),
        );
    });

    let err = context::with_resources(http_resources(), async {
        connector_discord::ops::DiscordSendMessage::invoke(&sample_input())
            .await
            .expect_err("bad token must fail")
    })
    .await;

    mock.assert();
    match err {
        ConnectorRuntimeError::HttpStatus { status, body } => {
            assert_eq!(status, 401);
            assert!(body.contains("Invalid Webhook Token"), "got: {body}");
        }
        other => panic!("expected HttpStatus error, got: {other}"),
    }

    let node_err = context::with_resources(http_resources(), async {
        discord_send_message(sample_input())
            .await
            .expect_err("bad token must fail through the action")
    })
    .await;
    let message = node_err.to_string();
    assert!(message.contains("401"), "got: {message}");
    assert!(message.contains("Invalid Webhook Token"), "got: {message}");
}

#[tokio::test]
async fn send_message_with_missing_auth_fails_actionably_without_calling_api() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(POST).path_contains("/api/webhooks/");
        then.status(204);
    });

    let err = context::with_resources(http_resources(), async {
        connector_discord::ops::DiscordSendMessage::invoke(&sample_input())
            .await
            .expect_err("missing auth must fail")
    })
    .await;

    assert_eq!(mock.hits(), 0);
    let message = err.to_string();
    assert!(message.contains("discord_webhook_auth"), "got: {message}");
    assert!(message.contains(AUTH_ENV), "got: {message}");
}
