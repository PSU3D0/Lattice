//! Capability honesty + idempotency evidence (connector verification harness).
//!
//! Since packet A2, every node executes against a `ScopedResources` view built
//! from its declared effect hints; undeclared access fails closed with a
//! structured CAP110 denial. These tests execute each connector op against a
//! scoped bag granting ONLY the hints its `ops::*::META` declares:
//!
//! - success under exactly the declared grants proves the declaration is
//!   *sufficient* (the op does not secretly need more);
//! - denial under an empty grant set proves the declaration is *load-bearing*
//!   (the op really uses the capability, so the hint is not an over-claim).
//!
//! The duplicate-injection test is the idempotency evidence for the one
//! Effectful op: composed with a dedupe reservation (the same gate
//! `Delivery::ExactlyOnce` requires at plan time, see kernel-plan
//! `check_exactly_once_requirements`), duplicate deliveries produce exactly
//! one outbound POST.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::dedupe::DedupeStore;
use capabilities::scoped::ScopedResources;
use capabilities::{ResourceAccess, ResourceBag, context};
use connector_github_issues::runtime::errors::ConnectorRuntimeError;
use connector_github_issues::runtime::transport::EnvConnectorRuntime;
use connector_github_issues::{
    GithubIssueCreateInput, GithubIssueGetInput, GithubIssueState, GithubIssuesListInput,
};
use connectors_std::dev::MemoryDedupeStore;
use dag_core::EffectHint;
use httpmock::Method::{GET, POST};
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GITHUB_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GITHUB_PAT";

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
                "connector.github.issues.test",
                "connector.github.issues",
            )),
    )
}

/// Scoped view granting exactly the hints declared in the op's metadata.
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

/// Scoped view granting nothing: every gated accessor must deny with CAP110.
fn scoped_to_nothing(op_meta: &dag_core::ConnectorOpMetadata) -> Arc<ScopedResources> {
    Arc::new(ScopedResources::new(op_meta.operation_id, full_bag(), []))
}

#[tokio::test]
async fn get_succeeds_under_exactly_declared_hints() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/repos/octo/demo/issues/7");
        then.status(200).json_body_obj(&serde_json::json!({
            "number": 7, "title": "scoped", "state": "open",
            "html_url": "https://example.test/issues/7"
        }));
    });

    let meta = &connector_github_issues::ops::GithubIssuesGet::META;
    let scoped = scoped_to_declared(meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let output = context::with_resources(view, async {
        connector_github_issues::ops::GithubIssuesGet::invoke(&GithubIssueGetInput {
            owner: "octo".to_string(),
            repo: "demo".to_string(),
            issue_number: 7,
        })
        .await
        .expect("get succeeds with only declared hints granted")
    })
    .await;

    mock.assert();
    assert_eq!(output.number, 7);
    assert!(
        scoped.take_denials().is_empty(),
        "declared hints must be sufficient: no CAP110 denials"
    );
}

#[tokio::test]
async fn list_succeeds_under_exactly_declared_hints_including_pagination() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let next_url = format!(
        "{}/repos/octo/demo/issues/page/2?per_page=100&state=open",
        server.base_url()
    );
    let page_one = server.mock(|when, then| {
        when.method(GET)
            .path("/repos/octo/demo/issues")
            .query_param("per_page", "100")
            .query_param("state", "open");
        then.status(200)
            .header("link", &format!("<{next_url}>; rel=\"next\""))
            .header("content-type", "application/json")
            .body(
                r#"[{"number":1,"title":"first","state":"open","html_url":"https://example.test/issues/1"}]"#,
            );
    });
    let page_two = server.mock(|when, then| {
        when.method(GET).path("/repos/octo/demo/issues/page/2");
        then.status(200)
            .header("content-type", "application/json")
            .body(
                r#"[{"number":2,"title":"second","state":"open","html_url":"https://example.test/issues/2"}]"#,
            );
    });

    let meta = &connector_github_issues::ops::GithubIssuesList::META;
    let scoped = scoped_to_declared(meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let output = context::with_resources(view, async {
        connector_github_issues::ops::GithubIssuesList::invoke(&GithubIssuesListInput {
            owner: "octo".to_string(),
            repo: "demo".to_string(),
            state: Some(GithubIssueState::Open),
            return_all: true,
            limit: None,
        })
        .await
        .expect("paginated list succeeds with only declared hints granted")
    })
    .await;

    page_one.assert();
    page_two.assert();
    assert_eq!(output.items.len(), 2);
    assert!(
        scoped.take_denials().is_empty(),
        "pagination follow-ups must stay within declared hints"
    );
}

#[tokio::test]
async fn create_succeeds_under_exactly_declared_hints() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "honesty-token");

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/repos/octo/demo/issues")
            .header("authorization", "Bearer honesty-token");
        then.status(201).json_body_obj(&serde_json::json!({
            "number": 11, "title": "scoped create", "state": "open",
            "html_url": "https://example.test/issues/11"
        }));
    });

    let meta = &connector_github_issues::ops::GithubIssuesCreate::META;
    let scoped = scoped_to_declared(meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let output = context::with_resources(view, async {
        connector_github_issues::ops::GithubIssuesCreate::invoke(&GithubIssueCreateInput {
            owner: "octo".to_string(),
            repo: "demo".to_string(),
            title: "scoped create".to_string(),
            body: None,
        })
        .await
        .expect("create succeeds with only declared hints granted")
    })
    .await;

    mock.assert();
    assert_eq!(output.number, 11);
    assert!(
        scoped.take_denials().is_empty(),
        "declared hints must be sufficient: no CAP110 denials"
    );
}

#[tokio::test]
async fn undeclared_access_is_denied_with_cap110_for_each_op() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "honesty-token");

    // Read op without its read grant: fails closed before any request.
    let get_meta = &connector_github_issues::ops::GithubIssuesGet::META;
    let scoped = scoped_to_nothing(get_meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();
    let err = context::with_resources(view, async {
        connector_github_issues::ops::GithubIssuesGet::invoke(&GithubIssueGetInput {
            owner: "octo".to_string(),
            repo: "demo".to_string(),
            issue_number: 1,
        })
        .await
        .expect_err("undeclared http_read must be denied")
    })
    .await;
    assert!(matches!(
        err,
        ConnectorRuntimeError::MissingHttpRead { action } if action == get_meta.operation_id
    ));
    let denials = scoped.take_denials();
    assert!(
        denials.iter().any(|denial| denial.capability == "http_read"),
        "expected an http_read denial, got: {denials:?}"
    );
    assert!(denials[0].message().contains("CAP110"));

    // Write op without its write grant: same failure mode.
    let create_meta = &connector_github_issues::ops::GithubIssuesCreate::META;
    let scoped = scoped_to_nothing(create_meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();
    let err = context::with_resources(view, async {
        connector_github_issues::ops::GithubIssuesCreate::invoke(&GithubIssueCreateInput {
            owner: "octo".to_string(),
            repo: "demo".to_string(),
            title: "denied".to_string(),
            body: None,
        })
        .await
        .expect_err("undeclared http_write must be denied")
    })
    .await;
    assert!(matches!(
        err,
        ConnectorRuntimeError::MissingHttpWrite { action } if action == create_meta.operation_id
    ));
    assert!(
        scoped
            .take_denials()
            .iter()
            .any(|denial| denial.capability == "http_write"),
        "expected an http_write denial"
    );
}

#[tokio::test]
async fn create_under_duplicate_injection_posts_exactly_once() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "honesty-token");

    let mock = server.mock(|when, then| {
        when.method(POST).path("/repos/octo/demo/issues");
        then.status(201).json_body_obj(&serde_json::json!({
            "number": 42, "title": "exactly once", "state": "open",
            "html_url": "https://example.test/issues/42"
        }));
    });

    let store = MemoryDedupeStore::new();
    let idempotency_key = b"connector.github.issues.create:octo/demo:exactly once";
    let ttl = Duration::from_secs(300);

    let (applied, blocked) = context::with_resources(full_bag(), async {
        let mut applied = 0usize;
        let mut blocked = 0usize;
        // Duplicate injection: the same logical request delivered three times.
        for _ in 0..3 {
            if store
                .put_if_absent(idempotency_key, ttl)
                .await
                .expect("dedupe reservation")
            {
                connector_github_issues::ops::GithubIssuesCreate::invoke(&GithubIssueCreateInput {
                    owner: "octo".to_string(),
                    repo: "demo".to_string(),
                    title: "exactly once".to_string(),
                    body: None,
                })
                .await
                .expect("gated create succeeds");
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

    // Certify the gate store itself with the shared idempotency harness.
    let report = testing_harness_idem::verify_dedupe_store(
        &store,
        b"harness-certification-key",
        Duration::from_millis(40),
        4,
    )
    .await;
    assert!(report.passed(), "dedupe store harness failed: {report:?}");
}
