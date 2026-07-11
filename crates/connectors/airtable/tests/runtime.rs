//! Canned-transport runtime contract for `connector.airtable.create_record`:
//! success path with full request assertions (path, bearer auth, composed
//! `fields` body), API error mapping, auth misconfiguration, and connector-op
//! reuse from a custom `def_node`.

use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_airtable::runtime::errors::ConnectorRuntimeError;
use connector_airtable::runtime::transport::EnvConnectorRuntime;
use connector_airtable::{AirtableCreateRecordInput, airtable_create_record};
use dag_core::{Effects, NodeError, NodeResult};
use dag_macros::def_node;
use httpmock::Method::POST;
use httpmock::MockServer;
use serde_json::json;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_AIRTABLE_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_AIRTABLE_TOKEN_AUTH";
const RECORDS_PATH: &str = "/v0/appTest0000000001/Transcripts";

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
                "connector.airtable.test",
                "connector.airtable",
            )),
    )
}

fn sample_input() -> AirtableCreateRecordInput {
    AirtableCreateRecordInput {
        base_id: "appTest0000000001".to_string(),
        table: "Transcripts".to_string(),
        fields: json!({ "Call ID": "call-1", "Sentiment": "positive" }),
        typecast: None,
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct MaybeSaveInput {
    should_save: bool,
    call_id: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct MaybeSaveOutput {
    saved: bool,
    record_id: Option<String>,
}

#[def_node(
    name = "MaybeSave",
    summary = "Custom node that reuses the Airtable create-record connector operation",
    connector_ops(connector_airtable::ops::AirtableCreateRecord)
)]
async fn maybe_save(input: MaybeSaveInput) -> NodeResult<MaybeSaveOutput> {
    if !input.should_save {
        return Ok(MaybeSaveOutput {
            saved: false,
            record_id: None,
        });
    }

    let created =
        connector_airtable::ops::AirtableCreateRecord::invoke(&AirtableCreateRecordInput {
            base_id: "appTest0000000001".to_string(),
            table: "Transcripts".to_string(),
            fields: json!({ "Call ID": input.call_id }),
            typecast: None,
        })
        .await
        .map_err(|err| NodeError::new(err.to_string()))?;

    Ok(MaybeSaveOutput {
        saved: true,
        record_id: Some(created.id),
    })
}

#[test]
fn custom_node_spec_auto_hoists_connector_op_requirements() {
    let spec = maybe_save_node_spec();
    assert_eq!(spec.effects, Effects::Effectful);
    assert_eq!(spec.determinism, dag_core::Determinism::BestEffort);
    assert!(
        spec.effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
    assert!(
        spec.connector_ops
            .iter()
            .any(|op| op.operation_id == "connector.airtable.create_record")
    );

    let generated = connector_airtable::airtable_create_record_node_spec();
    assert_eq!(generated.effects, Effects::Effectful);
    assert!(
        generated
            .effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
}

#[tokio::test]
async fn create_record_posts_fields_body_with_bearer_auth() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "airtable-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path(RECORDS_PATH)
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .header("authorization", "Bearer airtable-secret-token")
            .json_body_obj(&json!({
                "fields": { "Call ID": "call-1", "Sentiment": "positive" }
            }));
        then.status(200).json_body_obj(&json!({
            "id": "rec123",
            "createdTime": "2026-07-11T00:00:00.000Z",
            "fields": { "Call ID": "call-1" }
        }));
    });

    let output = context::with_resources(http_resources(), async {
        connector_airtable::ops::AirtableCreateRecord::invoke(&sample_input())
            .await
            .expect("create succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.id, "rec123");
    assert_eq!(
        output.created_time.as_deref(),
        Some("2026-07-11T00:00:00.000Z")
    );
}

#[tokio::test]
async fn custom_node_reuses_connector_operation() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "airtable-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST).path(RECORDS_PATH);
        then.status(200)
            .json_body_obj(&json!({ "id": "rec-2", "createdTime": "2026-07-11T00:00:00.000Z" }));
    });

    let output = context::with_resources(http_resources(), async {
        maybe_save(MaybeSaveInput {
            should_save: true,
            call_id: "call-2".to_string(),
        })
        .await
        .expect("custom node succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(
        output,
        MaybeSaveOutput {
            saved: true,
            record_id: Some("rec-2".to_string()),
        }
    );
}

#[tokio::test]
async fn create_record_maps_api_error_status_and_body() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "airtable-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST).path(RECORDS_PATH);
        then.status(422).json_body_obj(&json!({
            "error": { "type": "INVALID_REQUEST_UNKNOWN", "message": "Unknown field name" }
        }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_airtable::ops::AirtableCreateRecord::invoke(&sample_input())
            .await
            .expect_err("bad request must fail")
    })
    .await;

    mock.assert();
    match err {
        ConnectorRuntimeError::HttpStatus { status, body } => {
            assert_eq!(status, 422);
            assert!(body.contains("Unknown field name"), "got: {body}");
        }
        other => panic!("expected HttpStatus error, got: {other}"),
    }

    // The def_node action wrapper must keep both visible in the NodeError.
    let node_err = context::with_resources(http_resources(), async {
        airtable_create_record(sample_input())
            .await
            .expect_err("bad request must fail through the action")
    })
    .await;
    let message = node_err.to_string();
    assert!(message.contains("422"), "got: {message}");
    assert!(message.contains("Unknown field name"), "got: {message}");
}

#[tokio::test]
async fn create_record_with_missing_auth_fails_actionably_without_calling_api() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(POST).path(RECORDS_PATH);
        then.status(200).json_body_obj(&json!({ "id": "never" }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_airtable::ops::AirtableCreateRecord::invoke(&sample_input())
            .await
            .expect_err("missing auth must fail")
    })
    .await;

    assert_eq!(mock.hits(), 0);
    let message = err.to_string();
    assert!(message.contains("airtable_token_auth"), "got: {message}");
    assert!(message.contains(AUTH_ENV), "got: {message}");
}
