//! Capability honesty for the Drive family.
//!
//! - `search_files` must succeed under a scoped bag granting exactly its
//!   declared hints (`http_read` only) with zero CAP110 denials — the
//!   declaration is *sufficient*;
//! - under an empty grant set it must fail closed with `MissingHttpRead` and
//!   a recorded CAP110 denial — the declaration is *load-bearing*.
//!
//! This family slice has no Effectful op, so there is no idempotency-evidence
//! test here (see the guide: duplicate injection is required per Effectful op).

use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::scoped::ScopedResources;
use capabilities::{ResourceAccess, ResourceBag, context};
use connector_google_drive::GoogleDriveSearchFilesInput;
use connector_google_drive::runtime::errors::ConnectorRuntimeError;
use connector_google_drive::runtime::transport::EnvConnectorRuntime;
use dag_core::EffectHint;
use httpmock::Method::GET;
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_DRIVE_DEFAULT_BASE_URL";
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
                "connector.google.drive.test",
                "connector.google.drive",
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

fn sample_input() -> GoogleDriveSearchFilesInput {
    GoogleDriveSearchFilesInput {
        query: "trashed = false".to_string(),
        page_size: Some(10),
    }
}

#[tokio::test]
async fn search_files_succeeds_under_exactly_declared_hints() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "honesty-token");

    let mock = server.mock(|when, then| {
        when.method(GET).path("/drive/v3/files");
        then.status(200).json_body_obj(&serde_json::json!({
            "files": [{ "id": "doc-scoped" }]
        }));
    });

    let meta = &connector_google_drive::ops::GoogleDriveSearchFiles::META;
    let scoped = scoped_to_declared(meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let output = context::with_resources(view, async {
        connector_google_drive::ops::GoogleDriveSearchFiles::invoke(&sample_input())
            .await
            .expect("search succeeds with only declared hints granted")
    })
    .await;

    mock.assert();
    assert_eq!(output.items.len(), 1);
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
    let _auth = EnvGuard::set(AUTH_ENV, "honesty-token");

    let mock = server.mock(|when, then| {
        when.method(GET).path("/drive/v3/files");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "files": [] }));
    });

    let meta = &connector_google_drive::ops::GoogleDriveSearchFiles::META;
    let scoped = scoped_to_nothing(meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        connector_google_drive::ops::GoogleDriveSearchFiles::invoke(&sample_input())
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
