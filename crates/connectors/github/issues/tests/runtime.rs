use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_github_issues::runtime::transport::EnvConnectorRuntime;
use connector_github_issues::{
    GithubIssueCreateInput, GithubIssueGetInput, GithubIssueState, GithubIssuesListInput,
    github_issues_create, github_issues_get, github_issues_list,
};
use dag_core::{Effects, NodeError, NodeResult};
use dag_macros::def_node;
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
                "connector.github.issues.test",
                "connector.github.issues",
            )),
    )
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct MaybeCreateIssueInput {
    should_create: bool,
    owner: String,
    repo: String,
    title: String,
    body: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct MaybeCreateIssueOutput {
    created: bool,
    issue_number: Option<u64>,
}

#[def_node(
    name = "MaybeCreateIssue",
    summary = "Custom node that reuses the generated GitHub issue create connector operation",
    connector_ops(connector_github_issues::ops::GithubIssuesCreate)
)]
async fn maybe_create_issue(input: MaybeCreateIssueInput) -> NodeResult<MaybeCreateIssueOutput> {
    if !input.should_create {
        return Ok(MaybeCreateIssueOutput {
            created: false,
            issue_number: None,
        });
    }

    let created =
        connector_github_issues::ops::GithubIssuesCreate::invoke(&GithubIssueCreateInput {
            owner: input.owner,
            repo: input.repo,
            title: input.title,
            body: input.body,
        })
        .await
        .map_err(|err| NodeError::new(err.to_string()))?;

    Ok(MaybeCreateIssueOutput {
        created: true,
        issue_number: Some(created.number),
    })
}

#[test]
fn custom_node_spec_auto_hoists_connector_op_requirements() {
    let spec = maybe_create_issue_node_spec();
    assert_eq!(spec.effects, Effects::Effectful);
    assert_eq!(spec.determinism, dag_core::Determinism::BestEffort);
    assert!(
        spec.effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
    assert!(
        spec.connector_ops
            .iter()
            .any(|op| op.operation_id == "connector.github.issues.create")
    );

    let generated = connector_github_issues::github_issues_create_node_spec();
    assert_eq!(generated.effects, Effects::Effectful);
    assert_eq!(generated.determinism, dag_core::Determinism::BestEffort);
    assert!(
        generated
            .effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
    assert!(
        generated
            .connector_ops
            .iter()
            .any(|op| op.operation_id == "connector.github.issues.create")
    );
}

#[tokio::test]
async fn custom_node_reuses_generated_connector_operation() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "super-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/repos/octo/demo/issues")
            .header("authorization", "Bearer super-secret-token")
            .json_body_obj(&serde_json::json!({
                "title": "created from custom node",
                "body": "wrapped connector op"
            }));
        then.status(201).json_body_obj(&serde_json::json!({
            "number": 707,
            "title": "created from custom node",
            "state": "open",
            "html_url": "https://example.test/issues/707"
        }));
    });

    let output = context::with_resources(http_resources(), async {
        maybe_create_issue(MaybeCreateIssueInput {
            should_create: true,
            owner: "octo".to_string(),
            repo: "demo".to_string(),
            title: "created from custom node".to_string(),
            body: Some("wrapped connector op".to_string()),
        })
        .await
        .expect("custom node succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(
        output,
        MaybeCreateIssueOutput {
            created: true,
            issue_number: Some(707),
        }
    );
}

#[tokio::test]
async fn get_action_round_trips_against_mock_server() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/repos/octo/demo/issues/42");
        then.status(200).json_body_obj(&serde_json::json!({
            "number": 42,
            "title": "Connector substrate",
            "state": "open",
            "html_url": "https://example.test/issues/42"
        }));
    });

    let output = context::with_resources(http_resources(), async {
        github_issues_get(GithubIssueGetInput {
            owner: "octo".to_string(),
            repo: "demo".to_string(),
            issue_number: 42,
        })
        .await
        .expect("get succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.number, 42);
    assert_eq!(output.title, "Connector substrate");
}

#[tokio::test]
async fn list_action_follows_link_header_when_enabled() {
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
            .header("accept", "application/json")
            .header("x-github-api-version", "2022-11-28")
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
        when.method(GET)
            .path("/repos/octo/demo/issues/page/2")
            .header("accept", "application/json")
            .header("x-github-api-version", "2022-11-28")
            .query_param("per_page", "100")
            .query_param("state", "open");
        then.status(200)
            .header("content-type", "application/json")
            .body(
                r#"[{"number":2,"title":"second","state":"open","html_url":"https://example.test/issues/2"}]"#,
            );
    });

    let output = context::with_resources(http_resources(), async {
        github_issues_list(GithubIssuesListInput {
            owner: "octo".to_string(),
            repo: "demo".to_string(),
            state: Some(GithubIssueState::Open),
            return_all: true,
            limit: None,
        })
        .await
        .expect("list succeeds")
    })
    .await;

    page_one.assert();
    page_two.assert();
    assert_eq!(output.items.len(), 2);
    assert_eq!(output.items[0].number, 1);
    assert_eq!(output.items[1].number, 2);
}

#[tokio::test]
async fn create_action_uses_bearer_auth_env_override() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "super-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/repos/octo/demo/issues")
            .header("authorization", "Bearer super-secret-token")
            .json_body_obj(&serde_json::json!({
                "title": "created from lattice",
                "body": "hello from codegen"
            }));
        then.status(201).json_body_obj(&serde_json::json!({
            "number": 99,
            "title": "created from lattice",
            "state": "open",
            "html_url": "https://example.test/issues/99"
        }));
    });

    let output = context::with_resources(http_resources(), async {
        github_issues_create(GithubIssueCreateInput {
            owner: "octo".to_string(),
            repo: "demo".to_string(),
            title: "created from lattice".to_string(),
            body: Some("hello from codegen".to_string()),
        })
        .await
        .expect("create succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.number, 99);
    assert_eq!(output.title, "created from lattice");
}
