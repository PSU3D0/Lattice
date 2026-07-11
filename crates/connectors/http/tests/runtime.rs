//! Canned-transport runtime contract for connector.http: success decode modes,
//! the four outbound-auth kinds (Bearer, ApiKeyHeader, ApiKeyQuery, Basic),
//! §5 failure rows (HTTP101 non-2xx, HTTP102 bad JSON, HTTP103 typed mismatch,
//! HTTP104 non-UTF-8 text), 3xx-not-followed (relies on H2a `redirect: Off`),
//! and connector-op reuse from a custom `def_node`.
//!
//! `ENV_LOCK` is held across awaits deliberately to serialize the
//! process-global endpoint/auth env vars (same posture as the gmail/slack
//! harness).
#![allow(clippy::await_holding_lock)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::connector::OutboundAuthProfileDescriptor;
use capabilities::http::HttpMethod;
use capabilities::{ResourceBag, context};
use connector_http::runtime::errors::HttpConnectorError;
use connector_http::runtime::http_api::{HttpApi, HttpCall};
use connector_http::runtime::transport::EnvConnectorRuntime;
use connector_http::{
    HTTP_TARGET_AUTH_API_KEY_HEADER, HTTP_TARGET_AUTH_API_KEY_QUERY, HTTP_TARGET_AUTH_BASIC,
    HTTP_TARGET_AUTH_BEARER, HttpReadInput, HttpWriteInput, http_get,
};
use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;
use httpmock::Method::{GET, POST};
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_HTTP_TARGET_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_HTTP_TARGET_AUTH";

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
                "connector.http.test",
                "connector.http",
            )),
    )
}

fn read_input(path: &str) -> HttpReadInput {
    HttpReadInput {
        path: path.to_string(),
        ..Default::default()
    }
}

async fn send_get_with_auth(
    path: &str,
    auth: &'static OutboundAuthProfileDescriptor,
) -> Result<(), HttpConnectorError> {
    let api = HttpApi::for_target("connector.http.get").await?;
    let headers = BTreeMap::new();
    api.send(HttpCall {
        method: HttpMethod::Get,
        path_or_url: path,
        query: &[],
        headers: &headers,
        body: None,
        auth: Some(auth),
    })
    .await?;
    Ok(())
}

// ---- Success + decode modes ------------------------------------------------

#[tokio::test]
async fn get_json_decodes_success_body() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/things").query_param("page", "2");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "items": [1, 2, 3] }));
    });

    let output = context::with_resources(http_resources(), async {
        let mut input = read_input("/things");
        input.query = vec![("page".to_string(), "2".to_string())];
        connector_http::ops::HttpGet::invoke(&input)
            .await
            .expect("get succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.body, serde_json::json!({ "items": [1, 2, 3] }));
}

#[tokio::test]
async fn get_text_mode_returns_body_string() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/text");
        then.status(200).body("plain body");
    });

    let output = context::with_resources(http_resources(), async {
        connector_http::ops::HttpGet::invoke_text(&read_input("/text"))
            .await
            .expect("text succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.body, "plain body");
}

#[tokio::test]
async fn post_routes_write_and_sends_json_body() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/collect")
            .header("content-type", "application/json; charset=utf-8")
            .json_body_obj(&serde_json::json!({ "value": 9 }));
        then.status(201)
            .json_body_obj(&serde_json::json!({ "ok": true }));
    });

    let output = context::with_resources(http_resources(), async {
        let input = HttpWriteInput {
            path: "/collect".to_string(),
            body: Some(serde_json::json!({ "value": 9 })),
            ..Default::default()
        };
        connector_http::ops::HttpPost::invoke(&input)
            .await
            .expect("post succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.body, serde_json::json!({ "ok": true }));
}

// ---- Auth-header assertions (Bearer, ApiKeyHeader, ApiKeyQuery, Basic) ------

#[tokio::test]
async fn bearer_auth_sets_authorization_header() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "tok123");

    let mock = server.mock(|when, then| {
        when.method(GET)
            .path("/a")
            .header("authorization", "Bearer tok123");
        then.status(200).body("{}");
    });

    context::with_resources(http_resources(), async {
        send_get_with_auth("/a", &HTTP_TARGET_AUTH_BEARER)
            .await
            .expect("bearer send");
    })
    .await;
    mock.assert();
}

#[tokio::test]
async fn api_key_header_auth_sets_named_header() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "key456");

    let mock = server.mock(|when, then| {
        when.method(GET).path("/b").header("x-api-key", "key456");
        then.status(200).body("{}");
    });

    context::with_resources(http_resources(), async {
        send_get_with_auth("/b", &HTTP_TARGET_AUTH_API_KEY_HEADER)
            .await
            .expect("api key header send");
    })
    .await;
    mock.assert();
}

#[tokio::test]
async fn api_key_query_auth_appends_query_pair() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "qsecret");

    let mock = server.mock(|when, then| {
        when.method(GET)
            .path("/c")
            .query_param("api_key", "qsecret");
        then.status(200).body("{}");
    });

    context::with_resources(http_resources(), async {
        send_get_with_auth("/c", &HTTP_TARGET_AUTH_API_KEY_QUERY)
            .await
            .expect("api key query send");
    })
    .await;
    mock.assert();
}

#[tokio::test]
async fn basic_auth_sets_base64_authorization_header() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    // The `http.basic` secret handle stores `user:pass`.
    let _auth = EnvGuard::set(AUTH_ENV, "user:pass");

    // base64("user:pass") == "dXNlcjpwYXNz".
    let mock = server.mock(|when, then| {
        when.method(GET)
            .path("/d")
            .header("authorization", "Basic dXNlcjpwYXNz");
        then.status(200).body("{}");
    });

    context::with_resources(http_resources(), async {
        send_get_with_auth("/d", &HTTP_TARGET_AUTH_BASIC)
            .await
            .expect("basic send");
    })
    .await;
    mock.assert();
}

// ---- §5 failure rows -------------------------------------------------------

#[tokio::test]
async fn non_2xx_maps_to_http101() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/boom");
        then.status(503).body("upstream unavailable");
    });

    let err = context::with_resources(http_resources(), async {
        connector_http::ops::HttpGet::invoke(&read_input("/boom"))
            .await
            .expect_err("non-2xx must fail")
    })
    .await;

    mock.assert();
    assert_eq!(err.code(), Some("HTTP101"));
    match err {
        HttpConnectorError::NonSuccessStatus { status, excerpt } => {
            assert_eq!(status, 503);
            assert!(excerpt.contains("upstream unavailable"));
        }
        other => panic!("expected HTTP101, got {other}"),
    }
}

#[tokio::test]
async fn bad_json_body_maps_to_http102() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/badjson");
        then.status(200).body("this is not json");
    });

    let err = context::with_resources(http_resources(), async {
        connector_http::ops::HttpGet::invoke(&read_input("/badjson"))
            .await
            .expect_err("bad json must fail")
    })
    .await;

    mock.assert();
    assert_eq!(err.code(), Some("HTTP102"));
}

#[derive(Debug, serde::Deserialize)]
struct StrictShape {
    #[allow(dead_code)]
    required_field: String,
}

#[tokio::test]
async fn typed_mismatch_maps_to_http103() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/typed");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "other": 1 }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_http::ops::HttpGet::invoke_typed::<StrictShape>(&read_input("/typed"))
            .await
            .expect_err("typed mismatch must fail")
    })
    .await;

    mock.assert();
    assert_eq!(err.code(), Some("HTTP103"));
}

#[tokio::test]
async fn non_utf8_text_maps_to_http104() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/binary");
        then.status(200).body(vec![0xff, 0xfe, 0x00, 0x01]);
    });

    let err = context::with_resources(http_resources(), async {
        connector_http::ops::HttpGet::invoke_text(&read_input("/binary"))
            .await
            .expect_err("non-utf8 text must fail")
    })
    .await;

    mock.assert();
    assert_eq!(err.code(), Some("HTTP104"));
}

// ---- 3xx not followed (relies on H2a redirect: Off) ------------------------

#[tokio::test]
async fn redirect_is_not_followed_and_surfaces_as_http101_in_json_mode() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let redirect_mock = server.mock(|when, then| {
        when.method(GET).path("/redirect");
        then.status(302)
            .header("location", "https://evil.example/stolen");
    });

    let err = context::with_resources(http_resources(), async {
        connector_http::ops::HttpGet::invoke(&read_input("/redirect"))
            .await
            .expect_err("3xx must not be followed and must error in json mode")
    })
    .await;

    // Exactly one request: the redirect was surfaced, never followed.
    assert_eq!(redirect_mock.hits(), 1);
    match err {
        HttpConnectorError::NonSuccessStatus { status, .. } => assert_eq!(status, 302),
        other => panic!("expected HTTP101 with 302, got {other}"),
    }
}

#[tokio::test]
async fn redirect_full_response_carries_status_and_location() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let redirect_mock = server.mock(|when, then| {
        when.method(GET).path("/redirect2");
        then.status(301)
            .header("location", "https://elsewhere.example/");
    });

    let full = context::with_resources(http_resources(), async {
        connector_http::ops::HttpGet::invoke_full(&read_input("/redirect2"))
            .await
            .expect("full_response never errors on 3xx")
    })
    .await;

    assert_eq!(redirect_mock.hits(), 1);
    assert_eq!(full.status, 301);
    assert_eq!(
        full.headers.get("location").map(String::as_str),
        Some("https://elsewhere.example/")
    );
}

// ---- connector-op reuse in a custom node -----------------------------------

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct FetchInput {
    id: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct FetchOutput {
    name: String,
}

#[def_node(
    name = "FetchThing",
    summary = "Custom node that reuses connector.http.get with a typed decode",
    connector_ops(connector_http::ops::HttpGet)
)]
async fn fetch_thing(input: FetchInput) -> NodeResult<FetchOutput> {
    let out: FetchOutput = connector_http::ops::HttpGet::invoke_typed(&HttpReadInput {
        path: format!("/things/{}", input.id),
        ..Default::default()
    })
    .await
    .map_err(|err| NodeError::new(err.to_string()))?;
    Ok(out)
}

#[test]
fn custom_node_spec_hoists_connector_op_requirements() {
    let spec = fetch_thing_node_spec();
    assert_eq!(spec.effects, dag_core::Effects::ReadOnly);
    assert!(
        spec.effect_hints
            .contains(&capabilities::http::HINT_HTTP_READ)
    );
    assert!(
        spec.connector_ops
            .iter()
            .any(|op| op.operation_id == "connector.http.get")
    );
}

#[tokio::test]
async fn custom_node_reuses_typed_get() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/things/42");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "name": "widget" }));
    });

    let output = context::with_resources(http_resources(), async {
        fetch_thing(FetchInput {
            id: "42".to_string(),
        })
        .await
        .expect("custom node succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(
        output,
        FetchOutput {
            name: "widget".to_string()
        }
    );
}

// The def_node wrapper keeps failures actionable through NodeError.
#[tokio::test]
async fn action_wrapper_surfaces_http_code_in_node_error() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let _mock = server.mock(|when, then| {
        when.method(GET).path("/nope");
        then.status(404).body("missing");
    });

    let node_err = context::with_resources(http_resources(), async {
        http_get(read_input("/nope"))
            .await
            .expect_err("404 must fail through the action")
    })
    .await;
    assert!(node_err.to_string().contains("HTTP101"), "got: {node_err}");
}
