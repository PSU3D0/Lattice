//! Capability honesty + idempotency evidence for connector.http.
//!
//! - a GET op (http_read) and a POST op (http_write) succeed under a scoped bag
//!   granting exactly their declared hints, with zero CAP110 denials — the
//!   declaration is *sufficient*;
//! - under an empty grant set the GET fails closed with `MissingHttpRead` and
//!   the POST with `MissingHttpWrite`, each with a recorded CAP110 denial — the
//!   declaration is *load-bearing* (both are load-bearing, spec §4);
//! - duplicate injection through a dedupe reservation proves the Effectful POST
//!   composes with exactly-once: three deliveries, one outbound POST, plus a
//!   `verify_dedupe_store` harness certification.
#![allow(clippy::await_holding_lock)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::dedupe::DedupeStore;
use capabilities::scoped::ScopedResources;
use capabilities::{ResourceAccess, ResourceBag, context};
use connector_http::ops::{HttpGet, HttpPost};
use connector_http::runtime::errors::HttpConnectorError;
use connector_http::runtime::transport::EnvConnectorRuntime;
use connector_http::{HttpReadInput, HttpWriteInput};
use connectors_std::dev::MemoryDedupeStore;
use dag_core::EffectHint;
use httpmock::Method::{GET, POST};
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_HTTP_TARGET_BASE_URL";

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

fn full_bag() -> Arc<dyn ResourceAccess> {
    let client = Arc::new(ReqwestHttpClient::default());
    Arc::new(
        ResourceBag::default()
            .with_http_read(Arc::clone(&client))
            .with_http_write(client)
            .with_connector_runtime(Arc::new(EnvConnectorRuntime))
            .with_connector_scope(capabilities::connector::ConnectorBindingScope::new(
                "flow://tests",
                "honesty_test",
                "connector.http.test",
                "connector.http",
            )),
    )
}

fn scoped_to_declared(op_meta: &dag_core::ConnectorOpMetadata) -> Arc<ScopedResources> {
    let grants = op_meta
        .effect_hints
        .iter()
        .map(|hint| EffectHint::parse(hint).expect("declared hint parses"));
    Arc::new(ScopedResources::new(
        op_meta.operation_id,
        full_bag(),
        grants,
    ))
}

fn scoped_to_nothing(op_meta: &dag_core::ConnectorOpMetadata) -> Arc<ScopedResources> {
    Arc::new(ScopedResources::new(op_meta.operation_id, full_bag(), []))
}

fn read_input() -> HttpReadInput {
    HttpReadInput {
        path: "/read".to_string(),
        ..Default::default()
    }
}

fn write_input() -> HttpWriteInput {
    HttpWriteInput {
        path: "/write".to_string(),
        body: Some(serde_json::json!({ "event": "signup" })),
        ..Default::default()
    }
}

// ---- Declaration is sufficient (zero denials) ------------------------------

#[tokio::test]
async fn get_succeeds_under_exactly_declared_http_read() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let mock = server.mock(|when, then| {
        when.method(GET).path("/read");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "ok": 1 }));
    });

    let scoped = scoped_to_declared(&HttpGet::META);
    let view: Arc<dyn ResourceAccess> = scoped.clone();
    context::with_resources(view, async {
        HttpGet::invoke(&read_input())
            .await
            .expect("get succeeds with only http_read granted");
    })
    .await;

    mock.assert();
    assert!(
        scoped.take_denials().is_empty(),
        "declared http_read must be sufficient: no CAP110 denials"
    );
}

#[tokio::test]
async fn post_succeeds_under_exactly_declared_http_write() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let mock = server.mock(|when, then| {
        when.method(POST).path("/write");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "ok": 1 }));
    });

    let scoped = scoped_to_declared(&HttpPost::META);
    let view: Arc<dyn ResourceAccess> = scoped.clone();
    context::with_resources(view, async {
        HttpPost::invoke(&write_input())
            .await
            .expect("post succeeds with only http_write granted");
    })
    .await;

    mock.assert();
    assert!(
        scoped.take_denials().is_empty(),
        "no CAP110 denials expected"
    );
}

// ---- Declaration is load-bearing (CAP110 on empty grants) ------------------

#[tokio::test]
async fn undeclared_http_read_is_denied_with_cap110() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let mock = server.mock(|when, then| {
        when.method(GET).path("/read");
        then.status(200).body("{}");
    });

    let scoped = scoped_to_nothing(&HttpGet::META);
    let view: Arc<dyn ResourceAccess> = scoped.clone();
    let err = context::with_resources(view, async {
        HttpGet::invoke(&read_input())
            .await
            .expect_err("undeclared http_read must be denied")
    })
    .await;

    assert_eq!(mock.hits(), 0, "denial must precede any request");
    assert!(matches!(
        err,
        HttpConnectorError::Runtime(
            connectors_std::errors::ConnectorRuntimeError::MissingHttpRead { action }
        ) if action == HttpGet::META.operation_id
    ));
    let denials = scoped.take_denials();
    assert!(
        denials.iter().any(|d| d.capability == "http_read"),
        "expected an http_read denial, got: {denials:?}"
    );
    assert!(denials[0].message().contains("CAP110"));
}

#[tokio::test]
async fn undeclared_http_write_is_denied_with_cap110() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let mock = server.mock(|when, then| {
        when.method(POST).path("/write");
        then.status(200).body("{}");
    });

    let scoped = scoped_to_nothing(&HttpPost::META);
    let view: Arc<dyn ResourceAccess> = scoped.clone();
    let err = context::with_resources(view, async {
        HttpPost::invoke(&write_input())
            .await
            .expect_err("undeclared http_write must be denied")
    })
    .await;

    assert_eq!(mock.hits(), 0, "denial must precede any request");
    assert!(matches!(
        err,
        HttpConnectorError::Runtime(
            connectors_std::errors::ConnectorRuntimeError::MissingHttpWrite { action }
        ) if action == HttpPost::META.operation_id
    ));
    let denials = scoped.take_denials();
    assert!(
        denials.iter().any(|d| d.capability == "http_write"),
        "expected an http_write denial, got: {denials:?}"
    );
    assert!(denials[0].message().contains("CAP110"));
}

// ---- Duplicate injection: exactly-once POST --------------------------------

#[tokio::test]
async fn post_under_duplicate_injection_posts_exactly_once() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let mock = server.mock(|when, then| {
        when.method(POST).path("/write");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "ok": 1 }));
    });

    let store = MemoryDedupeStore::new();
    let idempotency_key = b"connector.http.post:/write:signup";
    let ttl = Duration::from_secs(300);

    let (applied, blocked) = context::with_resources(full_bag(), async {
        let mut applied = 0usize;
        let mut blocked = 0usize;
        for _ in 0..3 {
            if store
                .put_if_absent(idempotency_key, ttl)
                .await
                .expect("dedupe reservation")
            {
                HttpPost::invoke(&write_input())
                    .await
                    .expect("gated post succeeds");
                applied += 1;
            } else {
                blocked += 1;
            }
        }
        (applied, blocked)
    })
    .await;

    assert_eq!(applied, 1);
    assert_eq!(blocked, 2);
    assert_eq!(mock.hits(), 1, "exactly one POST despite three deliveries");

    let report = testing_harness_idem::verify_dedupe_store(
        &store,
        b"harness-certification-key",
        Duration::from_millis(40),
        4,
    )
    .await;
    assert!(report.passed(), "dedupe store harness failed: {report:?}");
}
