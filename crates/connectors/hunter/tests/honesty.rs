//! Capability honesty evidence for the Hunter family.
//!
//! - `verify_email` must succeed under a scoped bag granting exactly its
//!   declared hints (`http_read` only) with zero CAP110 denials — the
//!   declaration is *sufficient*;
//! - under an empty grant set it must fail closed with `MissingHttpRead` and a
//!   recorded CAP110 denial — the declaration is *load-bearing*.
//!
//! `verify_email` is a ReadOnly op, so there is no Effectful exactly-once
//! dedupe evidence here (that belongs to the Effectful sinks in the s24 flow).

use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::scoped::ScopedResources;
use capabilities::{ResourceAccess, ResourceBag, context};
use connector_hunter::HunterVerifyEmailInput;
use connector_hunter::runtime::errors::ConnectorRuntimeError;
use connector_hunter::runtime::transport::EnvConnectorRuntime;
use dag_core::EffectHint;
use httpmock::Method::GET;
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_HUNTER_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_HUNTER_API_KEY_AUTH";

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
                "connector.hunter.test",
                "connector.hunter",
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

fn sample_input() -> HunterVerifyEmailInput {
    HunterVerifyEmailInput {
        email: "ada@leads.test".to_string(),
    }
}

#[tokio::test]
async fn verify_email_succeeds_under_exactly_declared_hints() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "HONESTY-key");

    let mock = server.mock(|when, then| {
        when.method(GET).path("/v2/email-verifier");
        then.status(200).json_body_obj(&serde_json::json!({
            "data": { "status": "valid", "result": "deliverable", "score": 88, "email": "ada@leads.test" }
        }));
    });

    let meta = &connector_hunter::ops::HunterVerifyEmail::META;
    let scoped = scoped_to_declared(meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let output = context::with_resources(view, async {
        connector_hunter::ops::HunterVerifyEmail::invoke(&sample_input())
            .await
            .expect("verify succeeds with only declared hints granted")
    })
    .await;

    mock.assert();
    assert_eq!(output.result, "deliverable");
    assert!(
        scoped.take_denials().is_empty(),
        "declared hints must be sufficient: no CAP110 denials"
    );
}

#[tokio::test]
async fn undeclared_read_access_is_denied_with_cap110() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "HONESTY-key");

    let mock = server.mock(|when, then| {
        when.method(GET).path("/v2/email-verifier");
        then.status(200).json_body_obj(&serde_json::json!({
            "data": { "status": "valid", "result": "deliverable", "score": 88, "email": "ada@leads.test" }
        }));
    });

    let meta = &connector_hunter::ops::HunterVerifyEmail::META;
    let scoped = scoped_to_nothing(meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        connector_hunter::ops::HunterVerifyEmail::invoke(&sample_input())
            .await
            .expect_err("undeclared http_read must be denied")
    })
    .await;

    assert_eq!(mock.hits(), 0, "denial must happen before any request");
    assert!(matches!(
        err,
        ConnectorRuntimeError::MissingHttpRead { action } if action == meta.operation_id
    ));
    let denials = scoped.take_denials();
    assert!(
        denials
            .iter()
            .any(|denial| denial.capability == "http_read"),
        "expected an http_read denial, got: {denials:?}"
    );
    assert!(denials[0].message().contains("CAP110"));
}
