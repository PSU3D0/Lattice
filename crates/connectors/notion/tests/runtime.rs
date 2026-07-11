//! Canned-transport runtime contract for `connector.notion.create_page`:
//! success path with full request assertions (path, bearer auth, pinned
//! Notion-Version header, composed `parent`+`properties` body), API error
//! mapping, auth misconfiguration, and connector-op reuse from a custom
//! `def_node`.

use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_notion::runtime::errors::ConnectorRuntimeError;
use connector_notion::runtime::notion_api::NOTION_VERSION;
use connector_notion::runtime::transport::EnvConnectorRuntime;
use connector_notion::{NotionCreatePageInput, notion_create_page};
use dag_core::{Effects, NodeError, NodeResult};
use dag_macros::def_node;
use httpmock::Method::POST;
use httpmock::MockServer;
use serde_json::json;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_NOTION_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_NOTION_API_AUTH";
const PAGES_PATH: &str = "/v1/pages";

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
                "connector.notion.test",
                "connector.notion",
            )),
    )
}

fn sample_input() -> NotionCreatePageInput {
    NotionCreatePageInput {
        database_id: "db-1".to_string(),
        properties: json!({
            "Name": { "title": [{ "text": { "content": "call summary" } }] }
        }),
        icon: None,
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct MaybeLogInput {
    should_log: bool,
    summary: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct MaybeLogOutput {
    logged: bool,
    page_id: Option<String>,
}

#[def_node(
    name = "MaybeLog",
    summary = "Custom node that reuses the Notion create-page connector operation",
    connector_ops(connector_notion::ops::NotionCreatePage)
)]
async fn maybe_log(input: MaybeLogInput) -> NodeResult<MaybeLogOutput> {
    if !input.should_log {
        return Ok(MaybeLogOutput {
            logged: false,
            page_id: None,
        });
    }

    let created = connector_notion::ops::NotionCreatePage::invoke(&NotionCreatePageInput {
        database_id: "db-1".to_string(),
        properties: json!({
            "Name": { "title": [{ "text": { "content": input.summary } }] }
        }),
        icon: None,
    })
    .await
    .map_err(|err| NodeError::new(err.to_string()))?;

    Ok(MaybeLogOutput {
        logged: true,
        page_id: Some(created.id),
    })
}

#[test]
fn custom_node_spec_auto_hoists_connector_op_requirements() {
    let spec = maybe_log_node_spec();
    assert_eq!(spec.effects, Effects::Effectful);
    assert_eq!(spec.determinism, dag_core::Determinism::BestEffort);
    assert!(
        spec.effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
    assert!(
        spec.connector_ops
            .iter()
            .any(|op| op.operation_id == "connector.notion.create_page")
    );

    let generated = connector_notion::notion_create_page_node_spec();
    assert_eq!(generated.effects, Effects::Effectful);
    assert!(
        generated
            .effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
}

#[tokio::test]
async fn create_page_posts_parent_and_properties_with_version_header() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "notion-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path(PAGES_PATH)
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .header("notion-version", NOTION_VERSION)
            .header("authorization", "Bearer notion-secret-token")
            .json_body_obj(&json!({
                "parent": { "database_id": "db-1" },
                "properties": {
                    "Name": { "title": [{ "text": { "content": "call summary" } }] }
                }
            }));
        then.status(200).json_body_obj(&json!({
            "object": "page",
            "id": "page-123",
            "url": "https://www.notion.so/page-123"
        }));
    });

    let output = context::with_resources(http_resources(), async {
        connector_notion::ops::NotionCreatePage::invoke(&sample_input())
            .await
            .expect("create succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.id, "page-123");
    assert_eq!(output.object.as_deref(), Some("page"));
    assert_eq!(
        output.url.as_deref(),
        Some("https://www.notion.so/page-123")
    );
}

#[tokio::test]
async fn custom_node_reuses_connector_operation() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "notion-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST).path(PAGES_PATH);
        then.status(200)
            .json_body_obj(&json!({ "object": "page", "id": "page-2" }));
    });

    let output = context::with_resources(http_resources(), async {
        maybe_log(MaybeLogInput {
            should_log: true,
            summary: "hi".to_string(),
        })
        .await
        .expect("custom node succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(
        output,
        MaybeLogOutput {
            logged: true,
            page_id: Some("page-2".to_string()),
        }
    );
}

#[tokio::test]
async fn create_page_maps_api_error_status_and_body() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "notion-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST).path(PAGES_PATH);
        then.status(400).json_body_obj(&json!({
            "object": "error", "status": 400, "code": "validation_error",
            "message": "body failed validation"
        }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_notion::ops::NotionCreatePage::invoke(&sample_input())
            .await
            .expect_err("bad request must fail")
    })
    .await;

    mock.assert();
    match err {
        ConnectorRuntimeError::HttpStatus { status, body } => {
            assert_eq!(status, 400);
            assert!(body.contains("body failed validation"), "got: {body}");
        }
        other => panic!("expected HttpStatus error, got: {other}"),
    }

    let node_err = context::with_resources(http_resources(), async {
        notion_create_page(sample_input())
            .await
            .expect_err("bad request must fail through the action")
    })
    .await;
    let message = node_err.to_string();
    assert!(message.contains("400"), "got: {message}");
    assert!(message.contains("body failed validation"), "got: {message}");
}

#[tokio::test]
async fn create_page_with_missing_auth_fails_actionably_without_calling_api() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(POST).path(PAGES_PATH);
        then.status(200).json_body_obj(&json!({ "id": "never" }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_notion::ops::NotionCreatePage::invoke(&sample_input())
            .await
            .expect_err("missing auth must fail")
    })
    .await;

    assert_eq!(mock.hits(), 0);
    let message = err.to_string();
    assert!(message.contains("notion_api_auth"), "got: {message}");
    assert!(message.contains(AUTH_ENV), "got: {message}");
}
