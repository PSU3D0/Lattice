//! SSRF / injection unit vectors (spec §10). Each guard is exercised directly
//! (pure functions, no network) plus one end-to-end assertion that a bad path
//! is rejected before any request leaves the process.
#![allow(clippy::await_holding_lock)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_http::runtime::errors::HttpConnectorError;
use connector_http::runtime::http_api::{
    assert_same_origin, ssrf_guard, validate_header_pair, validate_path,
};
use connector_http::runtime::transport::EnvConnectorRuntime;
use connector_http::{HttpReadInput, ops::HttpGet};
use httpmock::Method::GET;
use httpmock::MockServer;

fn code(result: Result<(), HttpConnectorError>) -> Option<&'static str> {
    result.err().and_then(|err| err.code())
}

// ---- Path validation (HTTP001) --------------------------------------------

#[test]
fn path_traversal_is_rejected() {
    assert_eq!(code(validate_path("/a/../../etc/passwd")), Some("HTTP001"));
    assert_eq!(code(validate_path("/..")), Some("HTTP001"));
}

#[test]
fn network_path_reference_double_slash_is_rejected() {
    assert_eq!(code(validate_path("//evil.example/x")), Some("HTTP001"));
}

#[test]
fn missing_leading_slash_is_rejected() {
    assert_eq!(code(validate_path("relative/path")), Some("HTTP001"));
}

#[test]
fn backslash_and_control_chars_are_rejected() {
    assert_eq!(code(validate_path("/a\\b")), Some("HTTP001"));
    assert_eq!(code(validate_path("/a\r\nHost: evil")), Some("HTTP001"));
    assert_eq!(code(validate_path("/a\u{0000}b")), Some("HTTP001"));
}

#[test]
fn ordinary_paths_pass() {
    assert!(validate_path("/v2/invoices/123").is_ok());
    assert!(validate_path("/search").is_ok());
}

// ---- Header hygiene (HTTP106) ----------------------------------------------

#[test]
fn crlf_in_header_value_is_rejected() {
    assert_eq!(
        code(validate_header_pair("X-Trace", "ok\r\nInjected: yes")),
        Some("HTTP106")
    );
}

#[test]
fn denylisted_credential_and_hop_headers_are_rejected() {
    for name in [
        "Authorization",
        "authorization",
        "Cookie",
        "Host",
        "Content-Length",
        "Transfer-Encoding",
        "Connection",
        "Proxy-Authorization",
    ] {
        assert_eq!(
            code(validate_header_pair(name, "value")),
            Some("HTTP106"),
            "header `{name}` must be denylisted"
        );
    }
}

#[test]
fn malformed_header_name_is_rejected() {
    assert_eq!(
        code(validate_header_pair("bad header", "v")),
        Some("HTTP106")
    );
    assert_eq!(code(validate_header_pair("", "v")), Some("HTTP106"));
}

#[test]
fn ordinary_headers_pass() {
    assert!(validate_header_pair("X-Request-Id", "abc-123").is_ok());
    assert!(validate_header_pair("Accept", "application/json").is_ok());
}

// ---- Origin assertion (HTTP105) --------------------------------------------

#[test]
fn origin_mismatch_is_rejected() {
    let result = assert_same_origin("https://evil.example/x", "https://api.trusted.example");
    assert_eq!(code(result), Some("HTTP105"));
}

#[test]
fn same_origin_passes() {
    assert!(
        assert_same_origin(
            "https://api.trusted.example/v2/x?y=1",
            "https://api.trusted.example/"
        )
        .is_ok()
    );
}

// ---- Tier-2 SSRF guard -----------------------------------------------------

#[test]
fn tier2_rejects_loopback_and_private_and_metadata() {
    for url in [
        "https://localhost/x",
        "https://sub.localhost/x",
        "https://127.0.0.1/x",
        "https://10.0.0.5/x",
        "https://192.168.1.1/x",
        "https://172.16.9.9/x",
        "https://169.254.169.254/latest/meta-data",
        "https://100.64.1.1/x",
        "https://[::1]/x",
        "https://metadata.google.internal/x",
    ] {
        assert!(
            ssrf_guard(url).is_err(),
            "expected SSRF rejection for `{url}`"
        );
    }
}

#[test]
fn tier2_rejects_non_https_scheme() {
    assert!(ssrf_guard("http://api.example/x").is_err());
}

#[test]
fn tier2_allows_ordinary_public_https_host() {
    assert!(ssrf_guard("https://api.public.example/v1/things?q=1").is_ok());
}

// ---- End-to-end: a bad path never leaves the process -----------------------

#[tokio::test]
async fn bad_path_is_rejected_before_any_request() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(GET);
        then.status(200).body("{}");
    });

    let client = Arc::new(ReqwestHttpClient::default());
    let bag = Arc::new(
        ResourceBag::default()
            .with_http_read(Arc::clone(&client))
            .with_http_write(client)
            .with_connector_runtime(Arc::new(EnvConnectorRuntime))
            .with_connector_scope(capabilities::connector::ConnectorBindingScope::new(
                "flow://tests",
                "ssrf",
                "connector.http.test",
                "connector.http",
            )),
    );

    static ENV_LOCK: Mutex<()> = Mutex::new(());
    let _lock = ENV_LOCK.lock().expect("lock");
    unsafe {
        std::env::set_var(
            "LATTICE_CONNECTOR_ENDPOINT_HTTP_TARGET_BASE_URL",
            server.base_url(),
        );
    }

    let err = context::with_resources(bag, async {
        let input = HttpReadInput {
            path: "/a/../secret".to_string(),
            ..Default::default()
        };
        let _ = BTreeMap::<String, String>::new();
        HttpGet::invoke(&input)
            .await
            .expect_err("path traversal must be rejected")
    })
    .await;

    assert_eq!(err.code(), Some("HTTP001"));
    assert_eq!(mock.hits(), 0, "nothing must leave the process");

    unsafe {
        std::env::remove_var("LATTICE_CONNECTOR_ENDPOINT_HTTP_TARGET_BASE_URL");
    }
}
